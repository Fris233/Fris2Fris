use std::net::UdpSocket;
use rand::{SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::Rng;

use Fris2Fris::protocol;

fn generatePseudorandomChunks(chunks:& mut [[u8; protocol::ChunkSize]; protocol::NumberOfChunks]){
    for i in 0..protocol::NumberOfChunks {
        let mut rng = ChaCha8Rng::seed_from_u64(233*i as u64);
        rng.fill_bytes(&mut chunks[i as usize]);
    }
}

fn main() {
    let chunks:& mut [[u8; protocol::ChunkSize]; protocol::NumberOfChunks] = & mut [[0; protocol::ChunkSize]; protocol::NumberOfChunks];
    generatePseudorandomChunks(chunks);
    let listener = UdpSocket::bind("127.0.0.1:9000").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    loop {
        let mut buffer = [0; protocol::PacketSize];
        let (size, sender) = listener.recv_from(&mut buffer).unwrap();
        if size > 0 {
            println!("Received from {}", sender);
            for it in 0..32 {
                print!("{} ", buffer[it]);
            }
            println!();

            if buffer[2] == protocol::MessageType::HANDSHAKE as u8{
                println!("Handshake");

                let message:& mut [u8; protocol::PacketSize] = & mut [0; protocol::PacketSize];
                protocol::addHeaders(message, protocol::MessageType::HANDSHAKE_RESPONSE,
                                     0, 0, 0);
                listener.send_to(message, sender).unwrap();
                println!("Sent Response!");
            } else if buffer[2] == protocol::MessageType::CHUNK_REQUEST as u8 {
                println!("ChunkRequest");

                let selectedChunk = buffer[3];
                
                let message:& mut [u8; protocol::PacketSize] = & mut [0; protocol::PacketSize];
                protocol::addHeaders(message, protocol::MessageType::CHUNK_RESPONSE,
                                     selectedChunk, 0, protocol::ChunkSize as u16);
                protocol::addData(message, chunks[selectedChunk as usize], protocol::ChunkSize as u16);
                listener.send_to(message, sender).unwrap();
                println!("Sent Response!");  
            }
        }
    }
}