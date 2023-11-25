use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::runtime::Runtime;
use asyncmsg::base::Packet;
use asyncmsg::tcp::Client;
use anyhow::Result;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio::time::interval;


async fn send_request(client: Arc<Client>) {
    loop {
        let body = Bytes::from("hello from client");
        let pack = Packet::new_req(1, "", body);
        // println!("send req");
        let rsp_result = client.send_packet_and_wait_response(pack, 3, 3).await;
        if rsp_result.is_err() {
            // println!("failed to receive rsp");
        } else {
            let rsp = rsp_result.unwrap();
            println!("receive rsp from server, cmd = {}, seq = {}, device_id = {}, body = {}", rsp.cmd(), rsp.seq(), rsp.device_id(), String::from_utf8_lossy(rsp.body()));
        }

        let mut interval = interval(Duration::from_millis(200));
        interval.tick().await;
    }
}

async fn recv_request(client: Arc<Client>) {
    loop {
        let receive_req_result = client.async_wait_request(2).await;
        if receive_req_result.is_err() {
            println!("failed to receive req");
        } else {
            let req = receive_req_result.unwrap();
            println!("receive req from server, cmd = {}, seq = {}, device_id = {}, body = {}", req.cmd(), req.seq(), req.device_id(), String::from_utf8_lossy(req.body()));

            let body = Bytes::from("hello");
            let rsp = Packet::new_rsp(req.cmd(), req.seq(), 0, req.device_id(), body);
            let send_rsp_result = client.send_packet(rsp).await;
            if send_rsp_result.is_err() {
                println!("failed to send rsp");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (stop_tx, mut stop_rx) = watch::channel(());

    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        loop {
            select! {
                _ = sigterm.recv() => println!("Recieve SIGTERM"),
                _ = sigint.recv() => println!("Recieve SIGTERM"),
            };
            stop_tx.send(()).unwrap();
        }
    });


    let device_id_str = String::from("device_id_test");
    let client = Arc::new(Client::new("localhost", 5555, &device_id_str));

    let handle = tokio::spawn(
        async move {
            loop {
                select! {
                    _ = stop_rx.changed() => break,
                    _ = send_request(client.clone()) => {
                    },
                    _ = recv_request(client.clone()) => {
                    },
                }
            }
        }
    );

    handle.await;
}