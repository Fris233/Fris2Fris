use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use rand::RngExt;

use Fris2Fris::protocol;
use Fris2Fris::protocol::MessageType;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
struct Peer {
    rtt: u16,
    id: u8,
    addr: SocketAddr,
    chunkBitMap: [u8; protocol::NumberOfChunks as usize/8],
    lastSentHeartbeat: Instant,
}

fn main() {
    /* region Definitions */
    let serverAddr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let socket = Arc::new(Mutex::new(UdpSocket::bind("0.0.0.0:0").unwrap()));
    let thread_socket = Arc::clone(&socket);

    let mut peerId: u8 = 0;
    let peers: Arc<Mutex<BinaryHeap<Reverse<Peer>>>> = Arc::new(Mutex::new(BinaryHeap::new()));
    let thread_peers = Arc::clone(&peers);

    let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
    let data:& mut [[u8; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize] = & mut [[0; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize];

    let chunkBitMap:& mut [u8; protocol::NumberOfChunks as usize/8] = & mut [0; protocol::NumberOfChunks as usize/8];

    let mut rng = rand::rng();
    let mut pool: Vec<u8> = (0..=(protocol::NumberOfChunks-1) as u8).collect();
    socket.lock().unwrap().set_read_timeout(Some(Duration::from_millis(protocol::Timeout as u64))).unwrap();
    /* endregion */

    /* region Handshake */
    protocol::addHeaders(message, protocol::MessageType::HANDSHAKE, 0, 1, 0);
    match socket.lock().unwrap().send_to(message, serverAddr) {
        _ => {}
    }
    println!("Sent Handshake!");

    loop {
        let mut buffer = [0; protocol::PacketSize as usize];
        match socket.lock().unwrap().recv_from(&mut buffer) {
            Ok((size, sender)) => {
                if buffer[2] == protocol::MessageType::HANDSHAKE_RESPONSE as u8{
                    if size > 0 {
                        println!("Received Handshake Response from {}", sender);
                        peerId = buffer[16];
                        println!("Peer ID: {}", peerId);
                        break;
                    }
                }
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                let _ = socket.lock().unwrap().send_to(message, serverAddr);
                println!("Sent Handshake Again!");
            }
            Err(_) => { println!("Error"); }
        }
    }
    /* endregion */

    /* region AwaitStart */
    loop {
        let buffer = &mut [0; protocol::PacketSize as usize];
        match socket.lock().unwrap().recv_from(buffer) {
            Ok((size, sender)) => {
                if size > 0{
                    if buffer[2] == protocol::MessageType::START_SIGNAL as u8 {
                        println!("Received Start Signal from {}", sender);
                        let payloadLength = ((buffer[5] as u16) << 8) | buffer[6] as u16;
                        let mut guard_peers = peers.lock().unwrap();
                        for i in 0..payloadLength as usize / 7 {
                            let id = buffer[16 + i*7];
                            if id == peerId {
                                continue;
                            }
                            let addr = SocketAddr::new(
                                IpAddr::V4(Ipv4Addr::new(buffer[16+i*7+1], buffer[16+i*7+2], buffer[16+i*7+3], buffer[16+i*7+4])),
                                ((buffer[16+i*7+5] as u16)<<8) | buffer[16+i*7+6] as u16
                            );
                            guard_peers.push(Reverse(Peer {
                                rtt: u16::MAX,
                                id,
                                addr,
                                chunkBitMap: [0; protocol::NumberOfChunks as usize/8],
                                lastSentHeartbeat: Instant::now()
                            }));
                        }
                        break;
                    }
                }
            }
            Err(_) => {
                println!("Error, No Start Signal Received!");
            }
        }
    }
    /* endregion */

    /* region Initial Heartbeats */
    message.fill(0);
    protocol::addHeaders(message, MessageType::PEER_HEARTBEAT, 0, peerId, 0);
    {
        let mut guard_peers = peers.lock().unwrap();
        let s = socket.lock().unwrap();
        let mut peer_vec = std::mem::take(&mut *guard_peers).into_vec();
        for Reverse(peer) in peer_vec.iter_mut() {
            peer.lastSentHeartbeat = Instant::now();
            let _ = s.send_to(message, peer.addr);
            println!("Sent Heartbeat to peer {}!", peer.id);
        }
        *guard_peers = BinaryHeap::from(peer_vec);
    }
    /* endregion */

    /* region Listener */
    let captured_peer_id = peerId;
    let listener = thread::spawn(move || {
        let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
        loop {
            let buffer = &mut [0; protocol::PacketSize as usize];
            let mut guard_socket = thread_socket.lock().unwrap();
            match guard_socket.recv_from(buffer) {
                Ok((size, _sender)) => {
                    if size > 0 {
                        let mut guard_peers = thread_peers.lock().unwrap();
                        if buffer[2] == protocol::MessageType::PEER_HEARTBEAT as u8 {
                            let peer_id = buffer[4];
                            println!("Received Heartbeat Message from peer {}!", peer_id);

                            if !guard_peers.iter().any(|Reverse(p)| p.id == peer_id) {
                                guard_peers.push(Reverse(Peer {
                                    rtt: u16::MAX,
                                    id: peer_id,
                                    addr: _sender,
                                    chunkBitMap: [0; protocol::NumberOfChunks as usize/8],
                                    lastSentHeartbeat: Instant::now()
                                }));
                                println!("Added peer {} to peers list!", peer_id);
                            }

                            message.fill(0);
                            protocol::addHeaders(message, MessageType::PEER_HEARTBEAT_RESPONSE, 0, captured_peer_id, 0);
                            let _ = guard_socket.send_to(message, _sender);
                            println!("Sent Heartbeat_Response to peer {}!", peer_id);
                            continue;
                        }
                        if buffer[2] == protocol::MessageType::PEER_HEARTBEAT_RESPONSE as u8 {
                            let peer_id = buffer[4];
                            let mut peer_vec = std::mem::take(&mut *guard_peers).into_vec();
                            if let Some(Reverse(peer)) = peer_vec.iter_mut().find(|Reverse(p)| p.id == peer_id) {
                                peer.rtt = peer.lastSentHeartbeat.elapsed().as_millis() as u16;
                                println!("Received Heartbeat_Response from peer {}, RTT = {}ms!", peer_id, peer.rtt);
                            }
                            *guard_peers = BinaryHeap::from(peer_vec);
                            continue;
                        }
                    }
                }
                Err(_) => {
                }
            }
        }
    });
    /* endregion */

    /* region Requesting and Receiving Chunks */
    loop {
    }
    /* endregion */
}

fn GetMissingChunks(chunkBitMap:&[u8; protocol::NumberOfChunks as usize/8], peers: &BinaryHeap<Reverse<Peer>>) -> BinaryHeap<Reverse<(u8, u8, Option<Peer>)>> {
    let mut missingChunks: BinaryHeap<Reverse<(u8, u8, Option<Peer>)>> = BinaryHeap::new(); // (availability in network, chunkID, Peer with least RTT having the chunk)
    for chunkNumber in 0..protocol::NumberOfChunks as u8 {
        if(chunkBitMap[chunkNumber as usize /8] & (1 << chunkNumber % 8) == 0) {
            let mut availability: u8 = 0;
            let mut bestPeer: Option<Peer> = None;
            for Reverse(peer) in peers {
                if(peer.chunkBitMap[chunkNumber as usize / 8] & (1 << chunkNumber % 8) != 0) {
                    availability += 1;
                    if(bestPeer.is_none() || bestPeer.unwrap().rtt > peer.rtt) {
                        bestPeer = Some(*peer);
                    }
                }
            }
            missingChunks.push(Reverse((availability, chunkNumber, bestPeer)))
        }
    }
    missingChunks
}