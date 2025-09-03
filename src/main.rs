use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use mini_redis::Command::{self, Set, Get};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket, addr).await;
        });
    }
}

async fn process(socket: TcpStream, addr: SocketAddr) {
    let mut connection = Connection::new(socket);
    let mut db = HashMap::new();

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?} from {:?}", frame, addr);

        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().to_vec());
                Frame::Simple("OK".to_string())
            },
            Get(cmd) => {
                if let Some(val) = db.get(cmd.key()) {
                    Frame::Bulk(val.clone().into())
                } else {
                    Frame::Null
                }
            },
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
