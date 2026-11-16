use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use rand::RngExt;

use Fris2Fris::protocol;
use Fris2Fris::protocol::{addData, MessageType, ProtocolID, ProtocolVersion};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
struct Peer {
    rtt: u16,
    id: u8,
    addr: SocketAddr,
    chunkBitMap: [u8; protocol::NumberOfChunks as usize/8],
    lastSentHeartbeat: Instant,
}

const SERVER_ADDR: &str = "127.0.0.1:9000";

fn main() {
    let duplicatesReceived = Arc::new(AtomicU16::new(0));
    let thread_duplicatesReceived = Arc::clone(&duplicatesReceived);

    let chunksUploaded = Arc::new(AtomicU16::new(0));
    let thread_chunksUploaded = Arc::clone(&chunksUploaded);

    let chunksFromServer = Arc::new(AtomicU16::new(0));
    let thread_chunksFromServer = Arc::clone(&chunksFromServer);

    let mut requestsSentToPeers: u16 = 0;
    let mut requestsSentToServer: u16 = 0;
    let mut startTime: Instant;

    /* region Definitions */
    let serverAddr: SocketAddr = SERVER_ADDR.parse().unwrap();
    let socket = Arc::new(Mutex::new(UdpSocket::bind("0.0.0.0:0").unwrap()));
    let thread_socket = Arc::clone(&socket);

    let mut peerId: u8 = 0;
    let peers: Arc<Mutex<BinaryHeap<Reverse<Peer>>>> = Arc::new(Mutex::new(BinaryHeap::new()));
    let thread_peers = Arc::clone(&peers);

    let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
    let data: Arc<Mutex<[[u8; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize]>> = Arc::new(Mutex::new([[0; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize]));
    let thread_data = Arc::clone(&data);

    let chunkBitMap: Arc<Mutex<[u8; protocol::NumberOfChunks as usize/8]>> = Arc::new(Mutex::new([0; protocol::NumberOfChunks as usize/8]));
    let thread_chunkBitMap = Arc::clone(&chunkBitMap);

    let mut rng = rand::rng();
    let mut pool_vec: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new((0..=(protocol::NumberOfChunks-1) as u8).collect()));
    let thread_pool_vec = Arc::clone(&pool_vec);

    let mut requestingFromServer: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let mut chunkRequestedFromServer: u8 = 0;
    let thread_requestingFromServer = Arc::clone(&requestingFromServer);

    let timeSinceLastRequest: &mut [Instant; protocol::NumberOfChunks as usize] = &mut [Instant::now() - Duration::from_millis(protocol::Timeout as u64); protocol::NumberOfChunks as usize];

    /* endregion */

    /* region Handshake */
    protocol::addHeaders(message, protocol::MessageType::HANDSHAKE, 0, 1, 0);
    {
        let guard_socket = socket.lock().unwrap();
        guard_socket.set_read_timeout(Some(Duration::from_millis(protocol::Timeout as u64))).unwrap();
        match guard_socket.send_to(message, serverAddr) {
            _ => {}
        }
        println!("Sent Handshake!");
    }

    loop {
        let mut buffer = [0; protocol::PacketSize as usize];
        let guard_socket = socket.lock().unwrap();
        match guard_socket.recv_from(&mut buffer) {
            Ok((size, sender)) => {
                if(buffer[0] != ProtocolID || buffer[1] != ProtocolVersion) {continue;}
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
                match guard_socket.send_to(message, serverAddr) {
                    _ => {}
                }
                println!("Sent Handshake Again!");
            }
            Err(_) => { println!("Error"); }
        }
    }
    /* endregion */

    /* region AwaitStart */
    loop {
        let buffer = &mut [0; protocol::PacketSize as usize];
        let guard_socket = socket.lock().unwrap();
        match guard_socket.recv_from(buffer) {
            Ok((size, sender)) => {
                if size > 0 {
                    if(buffer[0] != ProtocolID || buffer[1] != ProtocolVersion) {continue;}
                    if buffer[2] == protocol::MessageType::START_SIGNAL as u8 {
                        startTime = Instant::now();
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
                                ((buffer[16+i*7+5] as u16) << 8) | buffer[16+i*7+6] as u16
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

    /* region Listener */
    let capturedPeerId = peerId;
    let listener = thread::spawn(move || {
        let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
        loop {
            let buffer = &mut [0; protocol::PacketSize as usize];
            let mut guard_socket = thread_socket.lock().unwrap();
            match guard_socket.recv_from(buffer) {
                Ok((size, sender)) => {
                    if size > 0 {
                        if(buffer[0] != ProtocolID || buffer[1] != ProtocolVersion) {continue;}

                        if buffer[2] == protocol::MessageType::PEER_HEARTBEAT as u8 {
                            let peer_id = buffer[4];
                            println!("Received Heartbeat Message from peer {}!", peer_id);

                            {
                                let mut guard_peers = thread_peers.lock().unwrap();
                                if !guard_peers.iter().any(|Reverse(p)| p.id == peer_id) {
                                    guard_peers.push(Reverse(Peer {
                                        rtt: u16::MAX,
                                        id: peer_id,
                                        addr: sender,
                                        chunkBitMap: [0; protocol::NumberOfChunks as usize / 8],
                                        lastSentHeartbeat: Instant::now()
                                    }));
                                    println!("Added peer {} to peers list!", peer_id);
                                    //TODO: Measure RTT for that peer
                                }
                            }

                            message.fill(0);
                            protocol::addHeaders(message, MessageType::PEER_HEARTBEAT_RESPONSE, 0, capturedPeerId, protocol::NumberOfChunks/8);

                            let data: &mut [u8; protocol::ChunkSize as usize] = & mut [0; protocol::ChunkSize as usize];
                            let guard_chunkBitMap = thread_chunkBitMap.lock().unwrap();
                            data[0..protocol::NumberOfChunks as usize/8].copy_from_slice(guard_chunkBitMap[0..protocol::NumberOfChunks as usize/8].as_ref());
                            protocol::addData(message, data);

                            match guard_socket.send_to(message, sender) {
                                _ => {}
                            }
                            println!("Sent Heartbeat_Response to peer {}!", peer_id);
                            continue;
                        }

                        if buffer[2] == protocol::MessageType::PEER_HEARTBEAT_RESPONSE as u8 {
                            let peer_id = buffer[4];
                            let mut guard_peers = thread_peers.lock().unwrap();
                            let mut peer_vec = std::mem::take(&mut *guard_peers).into_vec();
                            if let Some(Reverse(peer)) = peer_vec.iter_mut().find(|Reverse(p)| p.id == peer_id) {
                                peer.rtt = peer.lastSentHeartbeat.elapsed().as_millis() as u16;
                                let payloadSize = ((buffer[5] as u16) << 8) | buffer[6] as u16;
                                peer.chunkBitMap[0..payloadSize as usize].copy_from_slice(buffer[16..16+payloadSize as usize].as_ref());
                                println!("Received Heartbeat_Response from peer {}, RTT = {} ms!", peer_id, peer.rtt);
                            }
                            *guard_peers = BinaryHeap::from(peer_vec);
                            continue;
                        }

                        if buffer[2] == protocol::MessageType::CHUNK_REQUEST as u8 {
                            let chunkNumber = buffer[3];

                            let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
                            protocol::addHeaders(message, protocol::MessageType::CHUNK_RESPONSE,
                                                 chunkNumber, capturedPeerId, protocol::ChunkSize);
                            addData(message, &thread_data.lock().unwrap()[chunkNumber as usize]);
                            match guard_socket.send_to(message, sender) {
                                _ => {}
                            }
                            //println!("Sent Chunk {} to peer {}!", chunkNumber, buffer[4]);
                            thread_chunksUploaded.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }

                        if buffer[2] == protocol::MessageType::CHUNK_RESPONSE as u8 {
                            let chunkNumber: u8 = buffer[3];
                            let mut guard_chunkBitMap = thread_chunkBitMap.lock().unwrap();
                            if(HaveChunk(&*guard_chunkBitMap, chunkNumber)) {
                                //println!("Ignored duplicate chunk {} from peer {}", chunkNumber, buffer[4]);
                                thread_duplicatesReceived.fetch_add(1, Ordering::SeqCst);
                                continue;
                            }

                            //println!("Received Chunk {} from peer {}", chunkNumber , buffer[4]);
                            /*for it in 0..64 {
                                print!("{} ", buffer[it]);
                            }
                            println!();*/

                            let mut guard_data = thread_data.lock().unwrap();
                            let payloadSize = ((buffer[5] as u16) << 8) | buffer[6] as u16;
                            guard_data[chunkNumber as usize][0..payloadSize as usize].copy_from_slice(buffer[16..16+payloadSize as usize].as_ref());

                            guard_chunkBitMap[(chunkNumber/8) as usize] |= (1 << (chunkNumber%8));

                            /*println!("bitmap: ");
                            for byte in guard_chunkBitMap.iter() {
                                print!("{:08b} ", byte);
                            }
                            println!();*/

                            {
                                let mut guard_pool_vec = thread_pool_vec.lock().unwrap();
                                if let Some(index) = guard_pool_vec.iter().position(|&x| x == chunkNumber) {
                                    guard_pool_vec.remove(index);
                                }
                            }

                            if(buffer[4] == 0) {
                                thread_requestingFromServer.store(false, Ordering::SeqCst);
                                thread_chunksFromServer.fetch_add(1, Ordering::SeqCst);
                            }

                            let mut guard_peers = thread_peers.lock().unwrap();
                            AdvertiseChunks(&*guard_chunkBitMap, &*guard_peers, capturedPeerId, &*guard_socket);

                            continue;
                        }

                        if buffer[2] == protocol::MessageType::CHUNK_BITMAP as u8 {
                            let peer_id = buffer[4];
                            let mut guard_peers = thread_peers.lock().unwrap();
                            let mut peer_vec = std::mem::take(&mut *guard_peers).into_vec();
                            if let Some(Reverse(peer)) = peer_vec.iter_mut().find(|Reverse(p)| p.id == peer_id) {
                                let payloadSize = ((buffer[5] as u16) << 8) | buffer[6] as u16;
                                peer.chunkBitMap[0..payloadSize as usize].copy_from_slice(buffer[16..16+payloadSize as usize].as_ref());
                                //println!("Received ChunkBitMap from peer {}!", peer_id);
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

    /* region Initial Heartbeats */
    message.fill(0);
    protocol::addHeaders(message, MessageType::PEER_HEARTBEAT, 0, peerId, 0);
    {
        let guard_socket = socket.lock().unwrap();
        let mut guard_peers = peers.lock().unwrap();
        let mut peer_vec = std::mem::take(&mut *guard_peers).into_vec();
        for Reverse(peer) in peer_vec.iter_mut() {
            peer.lastSentHeartbeat = Instant::now();
            match guard_socket.send_to(message, peer.addr) {
                _ => {}
            }
            println!("Sent Heartbeat to peer {}!", peer.id);
        }
        *guard_peers = BinaryHeap::from(peer_vec);
    }
    /* endregion */

    /* region Requesting and Receiving Chunks */
    loop {
        let peersSnapshot: Vec<Peer> = {
            let guard_peers = peers.lock().unwrap();
            guard_peers.iter().map(|Reverse(p)| *p).collect()
        };
        let chunkBitMapSnapshot = {
            let guard = chunkBitMap.lock().unwrap();
            *guard
        };
        let rarestChunk: (bool, Option<(u8, u8, Peer)>) = GetRarestChunk(&chunkBitMapSnapshot, &peersSnapshot, timeSinceLastRequest);

        if rarestChunk.0 == true {
            //TODO: Finished getting all chunks, will probably send some signal to the server here?
            println!("Finished Getting all chunks!\nStats:\nRequests Sent to server: {}\nRequests Sent to peers: {}\nChunks Uploaded: {}\n\nDuplicates Received: {}\nChunks received from Server: {}\nTime Spent: {} ms",
                     requestsSentToServer, requestsSentToPeers, chunksUploaded.load(Ordering::SeqCst), duplicatesReceived.load(Ordering::SeqCst), chunksFromServer.load(Ordering::SeqCst), startTime.elapsed().as_millis());
            break;
        }

        if(rarestChunk.1.is_none()) {
            if requestingFromServer.load(Ordering::SeqCst) {
                if timeSinceLastRequest[chunkRequestedFromServer as usize].elapsed().as_millis() > protocol::Timeout as u128 {
                    message.fill(0);
                    protocol::addHeaders(message, protocol::MessageType::CHUNK_REQUEST,
                                         chunkRequestedFromServer, peerId, 0);

                    let guard_socket = socket.lock().unwrap();
                    match guard_socket.send_to(message, SERVER_ADDR) {
                        Ok(_) => {
                            //requestingFromServer.store(true, Ordering::SeqCst);
                            timeSinceLastRequest[chunkRequestedFromServer as usize] = Instant::now();
                            //println!("Resent Request for Chunk {} from server", chunkRequestedFromServer);
                            requestsSentToServer += 1;
                        }
                        Err(_) => {}
                    }
                }
                continue;
            }

            let mut chunkNumber: u8 = 0;
            {
                let guard_pool_vec = pool_vec.lock().unwrap();
                if(guard_pool_vec.is_empty()) {continue;}
                let index = rng.random_range(0..guard_pool_vec.len());
                chunkNumber = guard_pool_vec[index];
            }

            if timeSinceLastRequest[chunkNumber as usize].elapsed().as_millis() < protocol::Timeout as u128 {continue;}

            message.fill(0);
            protocol::addHeaders(message, protocol::MessageType::CHUNK_REQUEST,
                                 chunkNumber, peerId, 0);

            let guard_socket = socket.lock().unwrap();
            match guard_socket.send_to(message, SERVER_ADDR) {
                Ok(_) => {
                    requestingFromServer.store(true, Ordering::SeqCst);
                    chunkRequestedFromServer = chunkNumber;
                    timeSinceLastRequest[chunkNumber as usize] = Instant::now();
                    //println!("Requested Chunk {} from server", chunkNumber);
                    requestsSentToServer += 1;
                }
                Err(_) => {}
            }
            continue;
        }

        let chunk = rarestChunk.1.unwrap();
        message.fill(0);
        protocol::addHeaders(message, protocol::MessageType::CHUNK_REQUEST,
                             chunk.1, peerId, 0);

        let guard_socket = socket.lock().unwrap();
        match guard_socket.send_to(message, chunk.2.addr) {
            Ok(_) => {
                //println!("Requested Chunk {} from peer {}", chunk.1, chunk.2.id);
                requestsSentToPeers += 1;
                timeSinceLastRequest[chunk.1 as usize] = Instant::now();
            }
            Err(_) => {}
        }
    }
    /* endregion */

    listener.join().unwrap();
}

fn GetRarestChunk(chunkBitMap: &[u8; protocol::NumberOfChunks as usize/8], peers: &Vec<Peer>, timeSinceLastRequest: &mut [Instant; protocol::NumberOfChunks as usize]) -> (bool, Option<(u8, u8, Peer)>) {
    let mut rarestChunk: Option<(u8, u8, Peer)> = None;
    let mut done: bool = true;
    for chunkNumber in 0..=(protocol::NumberOfChunks-1) as u8 {
        if(!HaveChunk(chunkBitMap, chunkNumber)) {
            done = false;
            if(timeSinceLastRequest[chunkNumber as usize].elapsed().as_millis() < protocol::Timeout as u128) {continue;}
            let mut availability: u8 = 0;
            let mut bestPeer: Option<Peer> = None;
            for peer in peers {
                if(HaveChunk(&peer.chunkBitMap, chunkNumber)) {
                    availability += 1;
                    if(bestPeer.is_none() || bestPeer.unwrap().rtt > peer.rtt) {
                        bestPeer = Some(*peer);
                    }
                }
            }
            if availability == 0 {continue;}
            if rarestChunk.is_none() || rarestChunk.unwrap().0 > availability {
                rarestChunk = Option::from((availability, chunkNumber, bestPeer.unwrap()));
            }
        }
    }
    (done, rarestChunk)
}

fn HaveChunk(chunkBitMap: &[u8; protocol::NumberOfChunks as usize/8], chunkNumber: u8) -> bool {(chunkBitMap[chunkNumber as usize / 8] & (1u8 << (chunkNumber % 8))) != 0}

fn AdvertiseChunks(chunkBitMap: &[u8; protocol::NumberOfChunks as usize/8], peers: &BinaryHeap<Reverse<Peer>>, peerId: u8, socket: &UdpSocket) {
    let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
    protocol::addHeaders(message, MessageType::CHUNK_BITMAP, 0, peerId, protocol::NumberOfChunks/8);

    let data: &mut [u8; protocol::ChunkSize as usize] = & mut [0; protocol::ChunkSize as usize];
    data[0..protocol::NumberOfChunks as usize/8].copy_from_slice(chunkBitMap[0..protocol::NumberOfChunks as usize/8].as_ref());
    protocol::addData(message, data);

    for Reverse(peer) in peers {
        match socket.send_to(message, peer.addr) {
            _ => {}
        }
        //println!("Sent ChunkBitMap to peer {}!", peer.id);
    }
}
