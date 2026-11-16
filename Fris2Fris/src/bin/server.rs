use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use ratelimit::Ratelimiter;
use rand::{SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::Rng;
use Fris2Fris::protocol;

fn main() {
    let rateLimiter = Ratelimiter::builder(6250).build().unwrap();
    let AwaitRateLimit = |messageSize: u64| {
        while let Err(err) = rateLimiter.try_wait_n(messageSize) {
            if let ratelimit::TryWaitError::Insufficient(negative_duration) = err {
                std::thread::sleep(negative_duration);
            }
        }
    };

    /* region Data Generation */
    let chunks = & mut [[0; protocol::ChunkSize]; protocol::NumberOfChunks as usize];
    generatePseudorandomChunks(chunks);

    println!("Data (64x64 grid):");
    for (i, chunk) in chunks.iter().enumerate() {
        if(i % 4 == 0) {println!();}
        for byte in chunk {
            print!("{:3} ", byte);
        }
    }
    println!();
    /* endregion */

    let mut numberOfChunksSent: u16 = 0;
    let mut peers: HashMap<u8, [u8; 6]> = HashMap::new(); //TODO: Handle peers disconnecting
    let mut currPeerID:u8 = 0;

    let listener = UdpSocket::bind("127.0.0.1:9000").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    loop {
        let mut buffer = [0; protocol::PacketSize];
        let (size, sender) = listener.recv_from(&mut buffer).unwrap();
        if size > 0 {
            if(buffer[0] != protocol::ProtocolID || buffer[1] != protocol::ProtocolVersion) {continue;}

            /* region HANDSHAKE */
            if buffer[2] == protocol::MessageType::HANDSHAKE as u8 {

                currPeerID += 1; //TODO: If the handshake response is lost, some issues will occur!

                let mut message = vec![0u8; protocol::HeaderSize + 1];
                protocol::CreateMessage(&mut message, protocol::MessageType::HANDSHAKE_RESPONSE, 0, 0, 1, &mut vec![currPeerID]);

                AwaitRateLimit(message.len() as u64);

                listener.send_to(&*message, sender).unwrap();
                println!("Sent Handshake response to peer {}!", currPeerID);

                let mut isFound: bool = false;
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
                if peers.len() == 4 {
                    for (id, peer) in peers.iter() {
                        let payloadSize: u16 = peers.len() as u16 * 7;

                        let mut message = vec![0; protocol::HeaderSize + payloadSize as usize];
                        let mut data = vec![0; payloadSize as usize];
                        for (i, (id, addr)) in peers.iter().enumerate() {
                            data[i*7] = *id;
                            data[i*7+1..i*7+7].copy_from_slice(addr[0..6].as_ref());
                        }

                        protocol::CreateMessage(&mut message, protocol::MessageType::START_SIGNAL, 0, 0, payloadSize, &mut data);

                        AwaitRateLimit(message.len() as u64);

                        listener.send_to(&*message, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(peer[0], peer[1], peer[2], peer[3])), ((peer[4] as u16)<<8) | peer[5] as u16)).unwrap();
                        println!("Sent Start Signal to peer {}!", id);
                    }
                    continue;
                }
                if currPeerID > 4{
                    let payloadSize: u16 = peers.len() as u16 * 7;

                    let mut message = vec![0; protocol::HeaderSize + payloadSize as usize];
                    let mut data = vec![0; payloadSize as usize];
                    for (i, (id, addr)) in peers.iter().enumerate() {
                        data[i*7] = *id;
                        data[i*7+1..i*7+7].copy_from_slice(addr[0..6].as_ref());
                    }

                    protocol::CreateMessage(&mut message, protocol::MessageType::START_SIGNAL, 0, 0, payloadSize, &mut data);

                    AwaitRateLimit(message.len() as u64);

                    listener.send_to(&*message, sender).unwrap();
                    println!("Sent Start Signal to peer {}!", currPeerID);
                }
                continue;
            }
            /* endregion */

            /* region CHUNK_REQUEST */
            if buffer[2] == protocol::MessageType::CHUNK_REQUEST as u8 {
                let selectedChunk = buffer[3];
                
                let mut message = vec![0; protocol::HeaderSize + protocol::ChunkSize];
                protocol::CreateMessage(&mut message, protocol::MessageType::CHUNK_RESPONSE, selectedChunk, 0, protocol::ChunkSize as u16, &mut Vec::from(chunks[selectedChunk as usize]));

                AwaitRateLimit(message.len() as u64);

                listener.send_to(&*message, sender).unwrap();
                numberOfChunksSent += 1;
                println!("Sent Chunk {} to peer {}! Server has now sent a total of {} chunks", selectedChunk, buffer[4], numberOfChunksSent);
            }
            /* endregion */
        }
    }
}

/* region Helper Functions */
fn generatePseudorandomChunks(chunks:& mut [[u8; protocol::ChunkSize]; protocol::NumberOfChunks as usize]){
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
/* endregion */
