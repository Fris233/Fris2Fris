use std::net::UdpSocket;

fn main() {
    let listener = UdpSocket::bind("127.0.0.1:9000").unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    loop {
        let mut buffer = [0; 1024];
        let (size, sender) = listener.recv_from(&mut buffer).unwrap();
        if size > 0 {
            println!("Received from {}", sender);
            println!("Message: {}", String::from_utf8_lossy(&buffer[..size]));
        }
    }
}