use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::thread;
use std::time::Duration;
use rand::{SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::Rng;

use Fris2Fris::protocol;
use Fris2Fris::protocol::addData;

fn generatePseudorandomChunks(chunks:& mut [[u8; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize]){
    for i in 0..protocol::NumberOfChunks {
        let mut rng = ChaCha8Rng::seed_from_u64(233*i as u64);
        rng.fill_bytes(&mut chunks[i as usize]);
    }
}

fn socket_to_bytes(addr: SocketAddr) -> Option<[u8; 6]> {
    match addr {
        SocketAddr::V4(v4) => {
            let ip = v4.ip().octets();
            let port = v4.port().to_be_bytes();
            Some([ip[0], ip[1], ip[2], ip[3], port[0], port[1]])
        }
        SocketAddr::V6(_) => None,
    }
}

fn main() {
    let chunks:& mut [[u8; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize] = & mut [[0; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize];
    generatePseudorandomChunks(chunks);
    let mut peers: HashMap<u8, [u8; 6]> = HashMap::new();
    let mut currPeerID:u8 = 0;
    let listener = UdpSocket::bind("127.0.0.1:9000").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    loop {
        let mut buffer = [0; protocol::PacketSize as usize];
        let (size, sender) = listener.recv_from(&mut buffer).unwrap();
        if size > 0 {
            println!("Received from {}", sender);
            for it in 0..32 {
                print!("{} ", buffer[it]);
            }
            println!();

            if buffer[2] == protocol::MessageType::HANDSHAKE as u8{
                println!("Handshake");

                let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
                protocol::addHeaders(message, protocol::MessageType::HANDSHAKE_RESPONSE,
                                     0, 0, 1);
                currPeerID += 1;
                addData(message, [currPeerID; protocol::ChunkSize as usize]);
                listener.send_to(message, sender).unwrap();
                println!("Sent Response!");

                let mut isFound:bool = false;
                let senderAddr = socket_to_bytes(sender).unwrap();
                for (_, addr) in peers.iter() {
                    if addr == &senderAddr{
                        isFound = true;
                        break;
                    }
                }
                if !isFound {
                    peers.insert(currPeerID, senderAddr);
                }
                if peers.len() == 4{
                    for (_, peer) in peers.iter() {
                        let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
                        protocol::addHeaders(message, protocol::MessageType::START_SIGNAL,
                                             0, 0, 0);
                        listener.send_to(message, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(peer[0], peer[1], peer[2], peer[3])), ((peer[4] as u16)<<8) | peer[5] as u16)).unwrap();
                        println!("Sent Response!");
                    }
                }
                if currPeerID > 4{
                    let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
                    protocol::addHeaders(message, protocol::MessageType::START_SIGNAL,
                                         0, currPeerID, 0);
                    listener.send_to(message, sender).unwrap();
                    println!("Sent Response!");
                }
            } else if buffer[2] == protocol::MessageType::CHUNK_REQUEST as u8 {
                println!("ChunkRequest");

                let selectedChunk = buffer[3];
                
                let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];
                protocol::addHeaders(message, protocol::MessageType::CHUNK_RESPONSE,
                                     selectedChunk, 0, protocol::ChunkSize);
                addData(message, chunks[selectedChunk as usize]);
                listener.send_to(message, sender).unwrap();
                println!("Sent Response!");  
            }
        }
    }
}