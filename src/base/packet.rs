use std::ffi::CString;
use bytes::{Buf, Bytes};
use std::io::{Cursor};
use bytebuffer::{ByteBuffer, Endian};
use std::sync::atomic::{AtomicUsize, Ordering};


const CRC8_TABLE: [u8; 256] = [
0x00, 0x5e, 0xbc, 0xe2, 0x61, 0x3f, 0xdd, 0x83, 0xc2, 0x9c, 0x7e, 0x20, 0xa3, 0xfd, 0x1f, 0x41,
0x9d, 0xc3, 0x21, 0x7f, 0xfc, 0xa2, 0x40, 0x1e, 0x5f, 0x01, 0xe3, 0xbd, 0x3e, 0x60, 0x82, 0xdc,
0x23, 0x7d, 0x9f, 0xc1, 0x42, 0x1c, 0xfe, 0xa0, 0xe1, 0xbf, 0x5d, 0x03, 0x80, 0xde, 0x3c, 0x62,
0xbe, 0xe0, 0x02, 0x5c, 0xdf, 0x81, 0x63, 0x3d, 0x7c, 0x22, 0xc0, 0x9e, 0x1d, 0x43, 0xa1, 0xff,
0x46, 0x18, 0xfa, 0xa4, 0x27, 0x79, 0x9b, 0xc5, 0x84, 0xda, 0x38, 0x66, 0xe5, 0xbb, 0x59, 0x07,
0xdb, 0x85, 0x67, 0x39, 0xba, 0xe4, 0x06, 0x58, 0x19, 0x47, 0xa5, 0xfb, 0x78, 0x26, 0xc4, 0x9a,
0x65, 0x3b, 0xd9, 0x87, 0x04, 0x5a, 0xb8, 0xe6, 0xa7, 0xf9, 0x1b, 0x45, 0xc6, 0x98, 0x7a, 0x24,
0xf8, 0xa6, 0x44, 0x1a, 0x99, 0xc7, 0x25, 0x7b, 0x3a, 0x64, 0x86, 0xd8, 0x5b, 0x05, 0xe7, 0xb9,
0x8c, 0xd2, 0x30, 0x6e, 0xed, 0xb3, 0x51, 0x0f, 0x4e, 0x10, 0xf2, 0xac, 0x2f, 0x71, 0x93, 0xcd,
0x11, 0x4f, 0xad, 0xf3, 0x70, 0x2e, 0xcc, 0x92, 0xd3, 0x8d, 0x6f, 0x31, 0xb2, 0xec, 0x0e, 0x50,
0xaf, 0xf1, 0x13, 0x4d, 0xce, 0x90, 0x72, 0x2c, 0x6d, 0x33, 0xd1, 0x8f, 0x0c, 0x52, 0xb0, 0xee,
0x32, 0x6c, 0x8e, 0xd0, 0x53, 0x0d, 0xef, 0xb1, 0xf0, 0xae, 0x4c, 0x12, 0x91, 0xcf, 0x2d, 0x73,
0xca, 0x94, 0x76, 0x28, 0xab, 0xf5, 0x17, 0x49, 0x08, 0x56, 0xb4, 0xea, 0x69, 0x37, 0xd5, 0x8b,
0x57, 0x09, 0xeb, 0xb5, 0x36, 0x68, 0x8a, 0xd4, 0x95, 0xcb, 0x29, 0x77, 0xf4, 0xaa, 0x48, 0x16,
0xe9, 0xb7, 0x55, 0x0b, 0x88, 0xd6, 0x34, 0x6a, 0x2b, 0x75, 0x97, 0xc9, 0x4a, 0x14, 0xf6, 0xa8,
0x74, 0x2a, 0xc8, 0x96, 0x15, 0x4b, 0xa9, 0xf7, 0xb6, 0xe8, 0x0a, 0x54, 0xd7, 0x89, 0x6b, 0x35,
];

fn calc_crc8(data: &[u8]) -> u8 {
    let mut val: u8 = 0x77;
    for byte in data {
        val = val ^ byte;
        val = CRC8_TABLE[val as usize];
    }

    return val;
}

const DEVICE_ID_SIZE: usize = 64;
const PACKET_BEGIN_FLAG: u8 = 0x55;

// #[repr(packed(1))]
// struct PacketHeader {
//     flag: u8,
//     cmd: u32,
//     seq: u32,
//     rsp: u8,
//     ec: u32,
//     device_id_len: u32,
//     device_id: [u8; DEVICE_ID_SIZE],
//     body_len: u32,
//     crc: u8,
// }

const HEADER_LENGTH : usize = 87;//size_of::<PacketHeader>();
const MAX_BODY_LENGTH: usize = 16*1024;
static SEQUENCE: AtomicUsize = AtomicUsize::new(1);

pub struct Packet {
    cmd: u32,
    seq: u32,
    rsp: bool,
    ec: u32,
    device_id: String,
    body: Bytes,
}

impl Packet {
    pub fn new_req(cmd: u32, device_id: &str, body: Bytes) -> Packet {
        let seq = SEQUENCE.fetch_add(1, Ordering::SeqCst) as u32;
        let rsp = false;
        let ec : u32 = 0;
        Packet { cmd, seq, rsp, ec, device_id: device_id.to_string(), body}
    }

    pub fn new_rsp(cmd: u32, seq: u32, ec: u32, device_id: &str, body: Bytes) -> Packet {
        let rsp = true;
        Packet { cmd, seq, rsp, ec, device_id: device_id.to_string(), body}
    }

    pub fn from_bytes(buf: &mut Cursor<&[u8]>) -> Option<Packet> {
        if buf.get_ref().is_empty() {
            return None;
        }

        loop {
            let start = buf.position() as usize;
            let end = buf.get_ref().len() - 1;

            for i in start..end {
                if buf.get_ref()[i] == PACKET_BEGIN_FLAG {
                    buf.set_position(i as u64);
                    break;
                }
            }

            let buf_valid_len = buf.remaining();

            if buf_valid_len < HEADER_LENGTH as usize {
                return None;
            }

            let head_start_pos = buf.position() as usize;

            let _ = buf.get_u8();//flag
            let cmd = buf.get_u32();
            let seq = buf.get_u32();
            let rsp = buf.get_u8() != 0;
            let ec = buf.get_u32();

            let device_id_len = buf.get_u32();
            if device_id_len > DEVICE_ID_SIZE as u32 {
                continue;
            }

            let device_id_start = buf.position() as usize;
            let device_id_end_real = buf.position() as usize + device_id_len as usize;
            let device_id_end = buf.position() as usize + DEVICE_ID_SIZE;
            let device_id_bytes = Bytes::copy_from_slice(&buf.get_ref()[device_id_start..device_id_end_real]);
            let device_id = String::from_utf8_lossy(&device_id_bytes).to_string();
            buf.set_position(device_id_end as u64);

            let body_len = buf.get_u32();

            let crc_cal = calc_crc8(&buf.get_ref()[head_start_pos..buf.position() as usize]);
            let crc = buf.get_u8();
            if crc != crc_cal {
                continue;
            }

            if body_len as usize > MAX_BODY_LENGTH {
                continue;
            }

            let packet_len = HEADER_LENGTH + body_len as usize;
            if buf_valid_len < packet_len {
                return None;
            }

            let body_start = buf.position() as usize ;
            let body_end = body_start + body_len as usize;
            let body : Bytes = Bytes::copy_from_slice(&buf.get_ref()[body_start..body_end]);
            buf.set_position(body_end as u64);

            return Some(Packet { cmd, seq, rsp, ec, device_id, body});
        }
    }

    pub fn to_bytes(&self) -> ByteBuffer {
        let mut buffer: ByteBuffer = ByteBuffer::new();
        buffer.set_endian(Endian::BigEndian);//network order

        buffer.write_u8(PACKET_BEGIN_FLAG);
        buffer.write_u32(self.cmd);
        buffer.write_u32(self.seq);
        buffer.write_u8(self.rsp as u8);
        buffer.write_u32(self.ec);

        buffer.write_u32(self.device_id.len() as u32);
        let mut device_id_align_bytes = ByteBuffer::new();
        device_id_align_bytes.resize(DEVICE_ID_SIZE - self.device_id.len());
        buffer.write_bytes(self.device_id.as_bytes());
        buffer.write_bytes(device_id_align_bytes.as_bytes());

        buffer.write_u32(self.body.len() as u32);
        let crc: u8 = calc_crc8(buffer.as_bytes());
        buffer.write_u8(crc);
        buffer.write_bytes(&self.body);
        return buffer;
    }

    pub fn cmd(&self) -> u32 {
        self.cmd
    }

    pub fn seq(&self) -> u32 {
        self.seq
    }

    pub fn rsp(&self) -> bool {
        self.rsp
    }

    pub fn ec(&self) -> u32 {
        self.ec
    }

    pub fn set_device_id(&mut self, device_id: &str) {
        self.device_id = device_id.to_string();
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}