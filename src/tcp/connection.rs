use std::io::Cursor;
use std::sync::{Arc};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, RwLock};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::future::Future;
use std::mem::swap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use crate::base::Packet;
use crate::tcp::connection::ConnectionEvent::Closed;
use self::ConnectionEvent::{GotDeviceId, GotRequest};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Instant, interval, timeout};
use anyhow::{Error, Result};
use chrono::Local;
use tokio::runtime::{Handle};
use tokio::{select};
use tokio::task::JoinHandle;

const RECV_BUF_SIZE: usize = 128*1024;
const ACTIVE_CONNECTION_LIFETIME_SECONDS: u64 = 60;
const ACTIVE_CONNECTION_LIFETIME_CHECK_INTERVAL_SECONDS: u64 = 1;

static CONNECTION_ID: AtomicU32 = AtomicU32::new(1);


pub enum ConnectionEvent {
    Closed(u32, String),
    GotDeviceId(u32, String),
    GotRequest(u32, Packet),
}

pub struct Connection {
    connection_id: u32,
    stream_writer: RwLock<OwnedWriteHalf>,
    device_id: Arc<RwLock<String>>,
    rsp_chan_sender: Arc<RwLock<HashMap<u64, oneshot::Sender<Packet>>>>,
    stop_sender: Option<oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let mut stop_sender = None;
        let mut task_handle = None;
        swap(&mut stop_sender, &mut self.stop_sender);
        swap(&mut task_handle, &mut self.task_handle);

        if let Some(stop_sender) = stop_sender {
            let _ = stop_sender.send(());
        }

        let _ = thread::spawn(|| {
            async move {
                if let Some(task_handle) = task_handle {
                    let _ = task_handle.await;
                }
            }
        }).join();
    }
}

impl Connection {
    pub fn new(stream: TcpStream, device_id: &str, connection_event_sender: mpsc::Sender<ConnectionEvent>) -> Self {
        let (stream_reader, stream_writer) = stream.into_split();
        let (stop_sender, stop_receiver) = oneshot::channel();

        let mut conn = Connection {
            connection_id: CONNECTION_ID.fetch_add(1, Ordering::SeqCst),
            stream_writer: RwLock::new(stream_writer),
            device_id : Arc::new(RwLock::new(device_id.to_string())),
            rsp_chan_sender: Arc::new(RwLock::new(HashMap::new())),
            stop_sender: Some(stop_sender),
            task_handle: None,
        };

        conn.start(stream_reader, connection_event_sender, stop_receiver);
        return conn;
    }

    pub async fn send_packet(&self, packet: &Packet) -> Result<()> {
        let data = packet.to_bytes();
        println!("{}, send req2, cmd = {}, seq = {}, rsp = {}", Local::now(), packet.cmd(), packet.seq(), packet.rsp());
        Ok(self.stream_writer.write().await.write_all(data.as_bytes()).await?)
    }

    pub async fn send_packet_and_wait_response(&self, packet: &Packet, timeout_seconds: u64) -> Result<Packet> {
        let data = packet.to_bytes();
        self.stream_writer.write().await.write_all(data.as_bytes()).await?;

        println!("{}, send req, cmd = {}, seq = {}, rsp = {}", Local::now(), packet.cmd(), packet.seq(), packet.rsp());

        let (sender, receiver) = oneshot::channel();
        let packet_id = Self::get_packet_id(packet.cmd(), packet.seq());
        self.rsp_chan_sender.write().await.insert(packet_id, sender);

        println!("{}, send req, timeout begin", Local::now());
        let rsp_packet_result = timeout(Duration::from_secs(timeout_seconds), receiver).await;
        println!("{}, send req, timeout end", Local::now());

        self.rsp_chan_sender.write().await.remove(&packet_id);

        if rsp_packet_result.is_err() {
            return Err(Error::from(rsp_packet_result.err().unwrap()));
        }

        let rsp_packet_result= rsp_packet_result.unwrap();
        Ok(rsp_packet_result?)
    }

    pub fn get_connection_id(&self) -> u32 {
        return self.connection_id;
    }

    fn start(&mut self,  stream_reader: OwnedReadHalf, connection_event_sender: mpsc::Sender<ConnectionEvent>, stop_receiver: oneshot::Receiver<()>) {
        let connection_event_sender_check = connection_event_sender.clone();
        let connection_id: u32 = self.connection_id;
        let device_id = self.device_id.clone();
        let device_id_clone = self.device_id.clone();
        let rsp_chan_sender = self.rsp_chan_sender.clone();
        let last_recv_time = Arc::new(RwLock::new(Instant::now()));
        let last_recv_time_clone = last_recv_time.clone();

        let task_handle = Handle::current().spawn(
            async move {
                select! {
                    _ = stop_receiver => {
                        println!("stop_receiver fired");
                    },
                    _ = Self::receive_packet(connection_id, device_id, rsp_chan_sender, stream_reader, last_recv_time, connection_event_sender) => {
                        println!("receive_packet complete");
                    },
                    _ = Self::check(connection_id, device_id_clone, last_recv_time_clone, connection_event_sender_check) => {
                        println!("check complete");
                    },
                }
            }
        );

        let mut task_handle = Some(task_handle);
        swap(&mut task_handle, &mut self.task_handle);
    }

    fn receive_packet(connection_id: u32, device_id: Arc<RwLock<String>>
                      , rsp_chan_sender: Arc<RwLock<HashMap<u64, oneshot::Sender<Packet>>>>
                      , mut stream_reader: OwnedReadHalf
                      , last_recv_time: Arc<RwLock<Instant>>
                      , connection_event_sender: mpsc::Sender<ConnectionEvent>) -> impl Future<Output = ()> {
        async move {
            let mut read_buffer =  BytesMut::with_capacity(RECV_BUF_SIZE);
            loop {
                let received_packets = Self::process_packet(&mut read_buffer);
                let mut should_stop = false;

                for pack in received_packets {
                    println!("{}, recv pack, cmd = {}, seq = {}, device_id = {}, rsp = {}", Local::now(), pack.cmd(), pack.seq(), pack.device_id(), pack.rsp());
                    let pack_device_id = pack.device_id().to_string();
                    if pack_device_id.is_empty() {
                        println!("device_id is empty, discard it");
                        continue;
                    }

                    *last_recv_time.write().await = Instant::now();

                    if pack.rsp() {
                        let packet_id = Self::get_packet_id(pack.cmd(), pack.seq());
                        let mut rsp_chan_sender = rsp_chan_sender.write().await;
                        if !rsp_chan_sender.contains_key(&packet_id) {
                            println!("receive out of date rsp");
                            continue;
                        }

                        let rsp_chan_sender_for_packet = rsp_chan_sender.remove(&packet_id).unwrap();
                        if rsp_chan_sender_for_packet.send(pack).is_err() {
                            println!("send response to channel failed");
                        }
                    } else {
                        let mut device_id = device_id.write().await;

                        if device_id.is_empty() {
                            *device_id = pack_device_id;
                            if connection_event_sender.send(GotDeviceId(connection_id, device_id.to_string())).await.is_err() {
                                println!("send request to channel failed");
                                should_stop = true;
                                break;
                            }
                        } else if *device_id != pack_device_id {
                            println!("device_id invalid");
                            should_stop = true;
                            break;
                        }

                        if connection_event_sender.send(GotRequest(connection_id, pack)).await.is_err() {
                            println!("send request to channel failed");
                            should_stop = true;
                            break;
                        }
                    }
                }

                if should_stop {
                    println!("should_stop");
                    break;
                }

                let read_result = stream_reader.read_buf(&mut read_buffer).await;

                if read_result.is_err() {
                    println!("receive_buffer failed");
                    break;
                }

                let size = read_result.unwrap();
                // println!("recv size = {}", size);
                if size == 0 {
                    println!("socket closed");
                    break;
                }
            }

            let _ = connection_event_sender.send(Closed(connection_id, device_id.read().await.to_string())).await;
        }
    }

    fn check(connection_id: u32, device_id: Arc<RwLock<String>>, last_recv_time: Arc<RwLock<Instant>>
             , connection_event_sender: mpsc::Sender<ConnectionEvent>) -> impl Future<Output = ()> {
        async move {
            let mut interval = interval(Duration::from_secs(ACTIVE_CONNECTION_LIFETIME_CHECK_INTERVAL_SECONDS));
            loop {
                interval.tick().await;

                let now = Instant::now();
                let duration = now.duration_since(*last_recv_time.read().await);
                if duration.as_secs() > ACTIVE_CONNECTION_LIFETIME_SECONDS {
                    break;
                }
            }

            let _ = connection_event_sender.send(Closed(connection_id, device_id.read().await.to_string())).await;
        }
    }

    fn process_packet(read_buffer: &mut BytesMut) -> Vec<Packet> {
        let mut buf = Cursor::new(&read_buffer[..]);
        let mut packets: Vec<Packet> = Vec::new();

        loop {
            if let Some(pack) = Packet::from_bytes(&mut buf) {
                packets.push(pack);
                continue;
            } else {
                break;
            }
        }

        let consume_len = buf.position() as usize;
        read_buffer.advance(consume_len);
        return packets;
    }

    fn get_packet_id(cmd: u32, seq: u32) -> u64 {
        let mut packet_id : u64 = cmd as u64;
        packet_id = packet_id << 31;
        packet_id |= seq as u64;
        return packet_id;
    }
}