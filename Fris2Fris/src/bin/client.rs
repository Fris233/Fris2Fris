use std::net::UdpSocket;

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.send_to("Hello Mr. Server".as_bytes(), "127.0.0.1:9000").unwrap();
    println!("Sent message!");
}