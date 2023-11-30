use std::collections::HashMap;
use std::mem::swap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use anyhow::{Result, Error};
use chrono::Local;
use tokio::net::{TcpListener};
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use crate::base::Packet;
use crate::tcp::connection::{Connection, ConnectionEvent};

pub struct Server {
    id_conn: Arc<Mutex<HashMap<u32, Connection>>>,
    device_id_2_conn: Arc<Mutex<HashMap<String, Connection>>>,
    request_chan_sender: Arc<Mutex<HashMap<u32, mpsc::Sender<Packet>>>>,
    request_chan_receiver: Arc<Mutex<HashMap<u32, mpsc::Receiver<Packet>>>>,
    stop_sender: Option<oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl Drop for Server {
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

impl Server {
    pub fn new(server_port: u16) -> Self {
        let (stop_sender, stop_receiver) = oneshot::channel();

        let mut server = Server {
            id_conn: Arc::new(Mutex::new(HashMap::new())),
            device_id_2_conn: Arc::new(Mutex::new(HashMap::new())),
            request_chan_sender: Arc::new(Mutex::new(HashMap::new())),
            request_chan_receiver: Arc::new(Mutex::new(HashMap::new())),
            stop_sender: Some(stop_sender),
            task_handle: None,
        };

        server.start(server_port, stop_receiver);
        server
    }

    pub async fn send_packet(&self, mut packet: Packet) -> Result<()> {
        println!("{}, send rsp begin1, cmd = {}, seq = {}", Local::now(), packet.cmd(), packet.seq());

        let device_id_2_conn = self.device_id_2_conn.lock().await;

        if !device_id_2_conn.contains_key(packet.device_id()) {
            return Err(Error::msg("invalid device id"));
        }
        println!("{}, send rsp begin2, cmd = {}, seq = {}", Local::now(), packet.cmd(), packet.seq());
        device_id_2_conn.get(packet.device_id()).unwrap().send_packet(&packet).await
    }

    pub async fn send_packet_and_wait_response(&self, mut packet: Packet, timeout_seconds: u64, max_tries: u32) -> Result<Packet> {
        let mut interval = interval(Duration::from_millis(200));

        for _ in 0.. max_tries {
            let device_id_2_conn = self.device_id_2_conn.lock().await;
            if device_id_2_conn.contains_key(packet.device_id()) {
                let send_result = device_id_2_conn.get(packet.device_id()).unwrap().send_packet_and_wait_response(&packet, timeout_seconds).await;
                if send_result.is_ok() {
                    return send_result;
                }
            } else {
                interval.tick().await;
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

    fn start(&mut self, server_port: u16, stop_receiver: oneshot::Receiver<()>) {
        let id_conn = self.id_conn.clone();
        let id_conn_clone = self.id_conn.clone();
        let device_id_2_conn = self.device_id_2_conn.clone();
        let request_chan_sender = self.request_chan_sender.clone();
        let (conn_event_sender, conn_event_receiver) = mpsc::channel(128);

        let task_handle = Handle::current().spawn(
            async move {
                select! {
                    _ = stop_receiver => {
                        println!("stop_receiver fired");
                    },
                    _ = Self::accept(id_conn, server_port, conn_event_sender) => {
                        println!("receive_packet complete");
                    },
                    _ = Self::observe_connection_event(id_conn_clone, device_id_2_conn, request_chan_sender, conn_event_receiver) => {
                        println!("check complete");
                    },
                }
            }
        );

        let mut task_handle = Some(task_handle);
        swap(&mut task_handle, &mut self.task_handle);
    }

    async fn accept(id_conn: Arc<Mutex<HashMap<u32, Connection>>>, server_port: u16, conn_event_sender: mpsc::Sender<ConnectionEvent>) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", "localhost", server_port)).await?;

        let sender = conn_event_sender.clone();

        loop {
            let (socket, _) = listener.accept().await?;
            let device_id_empty = String::default();
            let conn = Connection::new( socket, &device_id_empty, conn_event_sender.clone());
            println!("new connection id = {}", conn.get_connection_id());
            id_conn.lock().await.insert(conn.get_connection_id(), conn);
        }
    }

    async fn observe_connection_event(id_conn: Arc<Mutex<HashMap<u32, Connection>>>
                                      , device_id_2_conn: Arc<Mutex<HashMap<String, Connection>>>
                                      , request_chan_sender: Arc<Mutex<HashMap<u32, mpsc::Sender<Packet>>>>
                                      , mut conn_event_receiver: mpsc::Receiver<ConnectionEvent>) -> Result<()> {
        while let Some(event) = conn_event_receiver.recv().await {
            // println!("connection event");
            match event {
                ConnectionEvent::Closed(conn_id, device_id) => {
                    println!("recv closed event, conn_id = {}, device_id = {}", conn_id, device_id);
                    id_conn.lock().await.remove(&conn_id);
                    device_id_2_conn.lock().await.remove(&device_id);
                }
                ConnectionEvent::GotDeviceId(conn_id, device_id) => {
                    println!("recv GotDeviceId event, conn_id = {}, device_id = {}", conn_id, device_id);

                    let conn = id_conn.lock().await.remove(&conn_id);
                    if let Some(conn) = conn {
                        device_id_2_conn.lock().await.insert(device_id, conn);
                    }
                }
                ConnectionEvent::GotRequest(conn_id, packet) => {
                    // println!("GotRequest event, conn_id = {}, device_id = {}", conn_id, packet.device_id());

                    if let Some(chan) = request_chan_sender.lock().await.get(&packet.cmd()) {
                        let _ = chan.send(packet).await;
                        // println!("send request to channel");
                    }
                }
            }
        }

        println!("observe_connection_event finished");
        Ok(())
    }
}