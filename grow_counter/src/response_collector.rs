use crate::*;
use core::panic;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub async fn take(tx: mpsc::Sender<Command>, msg_id: usize) -> Message {
    loop {
        let (resp_tx, resp_rx) = oneshot::channel();

        tx.send(Command::GetAndRemove {
            msg_id,
            resp: resp_tx,
        })
        .await
        .unwrap();

        if let Some(msg) = resp_rx.await.unwrap() {
            return msg;
        }
        tokio::task::yield_now().await;
    }
}

#[derive(Debug)]
pub enum Command {
    Put {
        msg_id: usize,
        msg: Message,
    },
    GetAndRemove {
        msg_id: usize,
        resp: oneshot::Sender<Option<Message>>,
    },
}
pub struct ResponseCollector {
    tx: mpsc::Sender<Command>,
}

impl ResponseCollector {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(200);

        tokio::spawn(async move {
            let mut responses = HashMap::new();

            while let Some(cmd) = rx.recv().await {
                use Command::*;

                match cmd {
                    Put { msg_id, msg } => {
                        responses.insert(msg_id, msg);
                    }
                    GetAndRemove { msg_id, resp } => {
                        if let Some(msg) = responses.remove_entry(&msg_id) {
                            if let Err(e) = resp.send(Some(msg.1)) {
                                panic!("sending GetAndRemove {:?}", e)
                            }
                        } else {
                            if let Err(e) = resp.send(None) {
                                panic!("sending GetAndRemove {:?}", e)
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub fn sender(&self) -> mpsc::Sender<Command> {
        self.tx.clone()
    }
}
