use crate::protocol;

#[repr(u8)]
pub  enum MessageType {
    HANDSHAKE = 100,
    HANDSHAKE_RESPONSE = 101,
    CHUNK_REQUEST = 200,
    CHUNK_RESPONSE = 201,
}

const ProtocolID:u8 = 233;
const ProtocolVersion:u8 = 001;
pub const Timeout:u16 = 500;
pub const PacketSize:usize = 1200;
pub const ChunkSize:usize = 1184;
pub const NumberOfChunks:usize = 256;

pub fn addHeaders(message:& mut [u8; PacketSize], message_type:MessageType, chunkNum:u8, peerID:u8, payloadLength:u16){
    message[0] = ProtocolID;
    message[1] = ProtocolVersion;
    message[2] = message_type as u8;
    message[3] = chunkNum;
    message[4] = peerID;
    message[5..7].copy_from_slice(payloadLength.to_be_bytes().as_ref());
}

pub fn addData(message:& mut [u8; PacketSize], data:[u8; protocol::ChunkSize], payloadLength:u16){
    message[16..(16+payloadLength) as usize].copy_from_slice(data[0..payloadLength as usize].as_ref());
}