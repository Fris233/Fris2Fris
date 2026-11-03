use crate::protocol;

#[repr(u8)]
pub enum MessageType {
    HANDSHAKE = 100,
    HANDSHAKE_RESPONSE = 101,
    CHUNK_REQUEST = 200, 
    CHUNK_RESPONSE = 201,
    FRIS= 233,
}

const ProtocolID:u8 = 233;
const ProtocolVersion:u8 = 001;
pub const Timeout:u16 = 500;
pub const PacketSize:u16 = 1200;
pub const ChunkSize:u16 = 1184;
pub const NumberOfChunks:u16 = 256;

pub fn addHeaders(message:& mut [u8; PacketSize as usize], message_type:MessageType, chunkNum:u8, peerID:u8, payloadLength:u16){
    message[0] = ProtocolID;
    message[1] = ProtocolVersion;
    message[2] = message_type as u8;
    message[3] = chunkNum; //TODO: verify whether chunk number should be header or in payload
    message[4] = peerID;
    message[5..7].copy_from_slice(payloadLength.to_be_bytes().as_ref());
}

pub fn addData(message:& mut [u8; PacketSize as usize], data:[u8; protocol::ChunkSize as usize], payloadLength:u16){
    message[16..(16+payloadLength) as usize].copy_from_slice(data[0..payloadLength as usize].as_ref());
}