use std::io::ErrorKind;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};
use rand::RngExt;

use Fris2Fris::protocol;

fn main() {
    let serverAddr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let message:& mut [u8; protocol::PacketSize as usize] = & mut [0; protocol::PacketSize as usize];

    /* region Handshake */
    protocol::addHeaders(message, protocol::MessageType::HANDSHAKE, 0, 1, 0);
    socket.set_read_timeout(Some(Duration::from_millis(protocol::Timeout as u64))).unwrap();
    match socket.send_to(message, serverAddr) {
        _ => {}
    }
    println!("Sent Handshake!");

    loop {
        let mut buffer = [0; protocol::PacketSize as usize];
        match socket.recv_from(&mut buffer) {
            Ok((size, sender)) => {
                if buffer[2] == protocol::MessageType::HANDSHAKE_RESPONSE as u8{
                    if size > 0 {
                        println!("Received Handshake Response from {}", sender);
                        for it in 0..32 {
                            print!("{} ", buffer[it]);
                        }
                        println!();
                        break;
                    }
                }
            }

            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                match socket.send_to(message, serverAddr) {
                    _ => {}
                }
                println!("Sent Handshake Again!");
            }

            Err(error) => {
                println!("Error");
            }
        }
    }
    /* endregion */

    //TODO: Handle concurrent chunk requesting and receiving
    let data:& mut [[u8; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize] = & mut [[0; protocol::ChunkSize as usize]; protocol::NumberOfChunks as usize];
    let bitMap:& mut [u8; protocol::NumberOfChunks as usize/8] = & mut [0; protocol::NumberOfChunks as usize/8];

    let mut rng = rand::rng();
    let mut pool: Vec<u8> = (0..=(protocol::NumberOfChunks-1) as u8).collect();

    loop {
        if !pool.is_empty() {
            let random_index = rng.random_range(0..pool.len());
            let selectedChunk = pool.swap_remove(random_index);

            println!("Selected {}", selectedChunk);

            message.fill(0);
            protocol::addHeaders(message, protocol::MessageType::CHUNK_REQUEST,
                                 selectedChunk, 1, 0);

            match socket.send_to(message, serverAddr) {
                _ => {}
            }

            loop {
                let mut buffer = [0; protocol::PacketSize as usize];
                match socket.recv_from(&mut buffer) {
                    Ok((size, sender)) => {
                        if buffer[2] == protocol::MessageType::CHUNK_RESPONSE as u8
                        && buffer[3] == selectedChunk  {
                            if size > 0 {
                                println!("Received Chunk {} from {}",selectedChunk , sender);
                                for it in 0..64 {
                                    print!("{} ", buffer[it]);
                                }
                                println!();
                                data[selectedChunk as usize][..].copy_from_slice(buffer[16..16+(((buffer[5] as u16)<<8) | buffer[6] as u16) as usize].as_ref());
                                bitMap[(selectedChunk/8) as usize] |= (1<<selectedChunk%8);
                                println!("bitmap: ");
                                for byte in bitMap.iter() {
                                    print!("{:08b}", byte);
                                }
                                println!();
                                break;
                            }
                        }
                    }

                    Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                        match socket.send_to(message, serverAddr) {
                            _ => {}
                        }
                        println!("Sent Request for chunk {} Again!", selectedChunk);
                    }

                    Err(error) => {
                        println!("Error");
                    }
                }
            }
        }else {
            break;
        }
    }
}