use crate::protocol;

#[repr(u8)]
pub enum MessageType {
    HANDSHAKE = 10,
    HANDSHAKE_RESPONSE = 11,
    CHUNK_REQUEST = 20,
    CHUNK_RESPONSE = 21,
    CHUNK_BITMAP = 22,
    START_SIGNAL = 30,
    KEEP_ALIVE = 40,
    PEER_HEARTBEAT = 50,
    PEER_HEARTBEAT_RESPONSE = 51,
    FRIS= 233,
}

pub const ProtocolID:u8 = 233;
pub const ProtocolVersion:u8 = 002;
pub const Timeout:u16 = 500;
pub const HeaderSize: usize = 16;
pub const ChunkSize: usize = 16;
pub const PacketSize: usize = 1024;
pub const NumberOfChunks:u16 = 256;

pub fn CreateMessage(message: &mut Vec<u8>, message_type:MessageType, chunkNum:u8, peerID:u8, payloadLength:u16, data: &mut Vec<u8>) {
    if(message.len() < (HeaderSize + payloadLength as usize) || data.len() != payloadLength as usize) {
        println!("Message length is wrong!");
        return;
    }
    message.fill(0);
    message[0] = ProtocolID;
    message[1] = ProtocolVersion;
    message[2] = message_type as u8;
    message[3] = chunkNum;
    message[4] = peerID;
    message[5..7].copy_from_slice(payloadLength.to_be_bytes().as_ref());
    message[(HeaderSize as usize)..(HeaderSize + payloadLength as usize)].copy_from_slice(data);
}