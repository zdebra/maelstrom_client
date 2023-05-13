use core::panic;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Commands {
    InitNode {
        id: String,
        peer_ids: Vec<String>,
    },
    GetID {
        resp: tokio::sync::oneshot::Sender<Option<String>>,
    },
}

pub async fn get_node_id(tx: mpsc::Sender<Commands>) -> String {
    let (resp_tx, resp_rx) = oneshot::channel();

    tx.send(Commands::GetID { resp: resp_tx }).await.unwrap();

    if let Some(msg) = resp_rx.await.unwrap() {
        return msg;
    } else {
        return String::new();
    }
}

pub struct NodeManager {
    tx: mpsc::Sender<Commands>,
}

impl NodeManager {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(200);

        tokio::spawn(async move {
            let mut node_id: Option<String> = None;

            while let Some(cmd) = rx.recv().await {
                use Commands::*;

                match cmd {
                    InitNode { id, .. } => {
                        node_id = Some(id);
                    }
                    GetID { resp } => {
                        if let Err(e) = resp.send(node_id.clone()) {
                            panic!("failed to send {:?}", e)
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub fn sender(&self) -> mpsc::Sender<Commands> {
        self.tx.clone()
    }
}
