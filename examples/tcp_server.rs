use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use asyncmsg::base::Packet;
use asyncmsg::tcp::{Server};
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio::time::interval;

async fn send_request(server: Arc<Server>) {
    loop {
        let body = Bytes::from("hello from server");
        let pack = Packet::new_req(2, "device_id_test", body);
        let rsp_result = server.send_packet_and_wait_response(pack, 3, 3).await;
        if rsp_result.is_err() {
            println!("failed to receive rsp");
        } else {
            let rsp = rsp_result.unwrap();
            println!("receive rsp from client, cmd = {}, seq = {}, device_id = {}, body = {}", rsp.cmd(), rsp.seq(), rsp.device_id(), String::from_utf8_lossy(rsp.body()));
        }

        let mut interval = interval(Duration::from_millis(200));
        interval.tick().await;
    }
}

async fn recv_request(server: Arc<Server>) {
    loop {
        let receive_req_result = server.async_wait_request(1).await;
        if receive_req_result.is_err() {
            println!("failed to receive req");
        } else {
            let req = receive_req_result.unwrap();
            println!("receive req from client, cmd = {}, seq = {}, device_id = {}, body = {}", req.cmd(), req.seq(), req.device_id(), String::from_utf8_lossy(req.body()));

            let body = Bytes::from("hello");
            let rsp = Packet::new_rsp(req.cmd(), req.seq(), 0, req.device_id(), body);
            let send_rsp_result = server.send_packet(rsp).await;
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
            }
            stop_tx.send(()).unwrap();
        }
    });


    let server = Arc::new(Server::new(5555));

    let handle = tokio::spawn(
        async move {
            loop {
                select! {
                    _ = stop_rx.changed() => break,
                    _ = recv_request(server.clone()) => {},
                    _ = send_request(server.clone()) => {}
                }
            }
        }
    );

    handle.await;
}