use std::collections::HashMap;
use std::future::Future;
use std::mem::swap;
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use crate::base::Packet;
use crate::tcp::Connection;
use anyhow::{Result, Error};
use tokio::net::TcpStream;
use tokio::runtime::{Handle};
use tokio::select;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use crate::tcp::connection::ConnectionEvent;

pub struct Client {
    device_id: String,
    conn: Arc<Mutex<Option<Connection>>>,
    request_chan_sender: Arc<Mutex<HashMap<u32, mpsc::Sender<Packet>>>>,
    request_chan_receiver: Arc<Mutex<HashMap<u32, mpsc::Receiver<Packet>>>>,
    stop_sender: Option<oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        let mut stop_sender = None;
        let mut task_handle = None;
        swap(&mut stop_sender, &mut self.stop_sender);
        swap(&mut task_handle, &mut self.task_handle);

        let _ = thread::spawn(|| {
            async move {
                if let Some(stop_sender) = stop_sender {
                    let _ = stop_sender.send(());
                }

                if let Some(task_handle) = task_handle {
                    let _ = task_handle.await;
                }
            }
        }).join();
    }
}

impl Client {
    pub fn new(server_host: &str, server_port: u16, device_id: &str) -> Self {
        let (stop_sender, stop_receiver) = oneshot::channel();

        let mut client = Client {
            device_id: device_id.to_string(),
            conn: Arc::new(Mutex::new(None)),
            request_chan_sender: Arc::new(Mutex::new(HashMap::new())),
            request_chan_receiver: Arc::new(Mutex::new(HashMap::new())),
            stop_sender: Some(stop_sender),
            task_handle: None,
        };

        client.start(server_host.to_string(), server_port, stop_receiver);
        client
    }

    pub async fn send_packet(&self, mut packet: Packet) -> Result<()> {
        if !self.reset_device_id_if_needed(&mut packet) {
            return Err(Error::msg("invalid device id"));
        }

        return if let Some(conn) = self.conn.lock().await.as_ref() {
            conn.send_packet(&packet).await
        } else {
            Err(Error::msg("disconnected"))
        }
    }

    pub async fn send_packet_and_wait_response(&self, mut packet: Packet, timeout_seconds: u64, max_tries: u32) -> Result<Packet> {
        if !self.reset_device_id_if_needed(&mut packet) {
            return Err(Error::msg("invalid device id"));
        }

        let mut interval = interval(Duration::from_millis(200));
        for _ in 0.. max_tries {
            {
                if let Some(conn) = self.conn.lock().await.as_ref() {
                    let send_result = conn.send_packet_and_wait_response(&packet, timeout_seconds).await;
                    if send_result.is_ok() {
                        return send_result;
                    }
                } else {
                    interval.tick().await;
                }
            }
        }

        Err(Error::msg("send_packet_and_wait_response failed"))
    }

    pub async fn async_wait_request(&self, cmd: u32) -> Result<Packet> {
        let mut req_receiver : Option<mpsc::Receiver<Packet>> = None;
        {
            let mut chan_receiver = self.request_chan_receiver.lock().await;
            if !chan_receiver.contains_key(&cmd) {
                let (sender, receiver) = mpsc::channel(128);
                req_receiver = Some(receiver);
                self.request_chan_sender.lock().await.insert(cmd, sender);
            } else {
                req_receiver = chan_receiver.remove(&cmd);
            }
        }

        let result = req_receiver.as_mut().unwrap().recv().await.ok_or(Error::msg("async_wait_request failed"));

        {
            let mut receiver : Option<mpsc::Receiver<Packet>> = None;
            swap(&mut receiver, &mut req_receiver);
            self.request_chan_receiver.lock().await.insert(cmd, receiver.unwrap());
        }

        return result;
    }

    fn start(&mut self, server_host: String, server_port: u16, stop_receiver: oneshot::Receiver<()>) {
        let conn = self.conn.clone();
        let conn_clone = self.conn.clone();
        let device_id = self.device_id.clone();
        let request_chan_sender = self.request_chan_sender.clone();
        let conn_event_receiver: Arc<Mutex<Option<mpsc::Receiver<ConnectionEvent>>>> = Arc::new(Mutex::new(None));
        let conn_event_receiver_clone = conn_event_receiver.clone();

        let task_handle = Handle::current().spawn(
            async move {
                select! {
                    _ = stop_receiver => {
                        println!("stop_receiver fired");
                    },
                    _ = Self::connect(conn, device_id, server_host, server_port, conn_event_receiver) => {
                        println!("receive_packet complete");
                    },
                    _ = Self::observe_connection_event(conn_clone, request_chan_sender, conn_event_receiver_clone) => {
                        println!("check complete");
                    },
                }
            }
        );

        let mut task_handle = Some(task_handle);
        swap(&mut task_handle, &mut self.task_handle);
    }

    fn connect(conn: Arc<Mutex<Option<Connection>>>, device_id: String, server_host: String, server_port: u16, conn_event_receiver: Arc<Mutex<Option<mpsc::Receiver<ConnectionEvent>>>>) -> impl Future<Output = ()> {
        async move {
            loop {
                if conn.lock().await.is_none() {
                    let connect_result = TcpStream::connect(format!("{}:{}", server_host, server_port)).await;
                    if connect_result.is_err() {
                        println!("connect error: {}", connect_result.err().unwrap());
                    } else {
                        println!("connect success");
                        let (sender, receiver) = mpsc::channel(32);
                        *conn_event_receiver.lock().await = Some(receiver);
                        *conn.lock().await = Some(Connection::new( connect_result.unwrap(), &device_id, sender));
                        println!("set connection some");
                    }
                }

                let mut interval = interval(Duration::from_millis(200));
                interval.tick().await;
            }
        }
    }

    fn observe_connection_event(conn: Arc<Mutex<Option<Connection>>>, request_chan_sender: Arc<Mutex<HashMap<u32, mpsc::Sender<Packet>>>>, conn_event_receiver: Arc<Mutex<Option<mpsc::Receiver<ConnectionEvent>>>>) -> impl Future<Output = ()> {
        async move {
            loop {
                let mut conn_event_receiver = conn_event_receiver.lock().await;
                if let Some(receiver) = conn_event_receiver.as_mut() {
                    while let Some(event) = receiver.recv().await {
                        match event {
                            ConnectionEvent::Closed(_, _) => {
                                println!("connection closed");
                                break;
                            }
                            ConnectionEvent::GotDeviceId(_, _) => {

                            }
                            ConnectionEvent::GotRequest(_, packet) => {
                                if let Some(sender) = request_chan_sender.lock().await.get(&packet.cmd()) {
                                    let _ = sender.send(packet).await;
                                }
                            }
                        }
                    }

                    println!("set connection none");
                    *conn.lock().await = None;
                    println!("set connection none2");
                    *conn_event_receiver = None;
                    println!("set connection none finished");
                }

                let mut interval = interval(Duration::from_millis(500));
                interval.tick().await;
            }
        }
    }

    fn reset_device_id_if_needed(&self, packet: &mut Packet) -> bool {
        if packet.device_id().is_empty() {
            packet.set_device_id(&self.device_id);
            return true;
        }

        return packet.device_id() == self.device_id;
    }
}