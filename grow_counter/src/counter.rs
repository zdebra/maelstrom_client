use crate::*;
use core::panic;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc::*;

#[derive(Debug)]
pub enum Commands {
    Read {
        resp: tokio::sync::oneshot::Sender<usize>,
    },
    Add {
        delta: usize,
    },
    Init {
        node_id: String,
    },
}

pub struct Counter {
    tx: Sender<Commands>,
}

impl Counter {
    pub fn sender(&self) -> Sender<Commands> {
        self.tx.clone()
    }

    pub fn new(msg_cnt: Arc<Mutex<usize>>, response_tx: Sender<RespColCommand>) -> Self {
        let (tx, mut rx) = channel(200);

        let mut interval_timer =
            tokio::time::interval(chrono::Duration::seconds(2).to_std().unwrap());

        tokio::spawn(async move {
            // local copy of a global counter based on the latest synchronization
            let mut global_counter_local_value = 0;

            // sum of all locally received additions
            let mut local_delta = 0;

            let mut nid: Option<String> = None;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if let Some(node_id) = nid.clone() {
                            eprintln!("[counter sync] counter synchronization...");
                            sync(node_id.clone(), msg_cnt.clone(), response_tx.clone(), &mut global_counter_local_value, &mut local_delta).await;
                        } else {
                            eprintln!("[counter sync] counter sync skipped")
                        }
                    }
                    Some(cmd) = rx.recv() => {
                        eprintln!("[counter sync] received counter command");
                        use Commands::*;

                        match cmd {
                            Read { resp } => {
                                resp.send(global_counter_local_value + local_delta).unwrap();
                            }
                            Add { delta } => {
                                local_delta += delta;
                            }
                            Init {node_id} => {
                                nid = Some(node_id);
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }
}

async fn sync(
    node_id: String,
    msg_cnt: Arc<Mutex<usize>>,
    response_tx: Sender<RespColCommand>,
    global_counter_local_value: &mut usize,
    local_delta: &mut usize,
) {
    loop {
        let read_id = new_msg_id(msg_cnt.clone());
        send_global_counter_read(read_id, node_id.clone());
        eprintln!("[counter sync] awaiting read response {}", read_id);

        let read_resp = take(response_tx.clone(), read_id).await;
        let global_counter = match read_resp.body.payload {
            Payload::ReadOk { value } => value,
            _ => panic!("[counter sync] unexpected response: {:?}", read_resp),
        };

        eprintln!("[counter sync] received global counter {}", global_counter);

        let new_global_counter = global_counter + *local_delta;
        let cas_id = {
            let id = new_msg_id(msg_cnt.clone());
            send_msg(Message {
                src: node_id.clone(),
                dest: SEQ_KV_SVC.to_string(),
                body: MessageBody {
                    msg_id: Some(id),
                    in_reply_to: None,
                    payload: Payload::Cas {
                        key: GLOBAL_COUNTER_KEY.to_string(),
                        from: global_counter,
                        to: new_global_counter,
                    },
                },
            });
            id
        };

        let cas_resp = take(response_tx.clone(), cas_id).await;
        match cas_resp.body.payload {
            Payload::CasOk => {
                // reset local counters
                *global_counter_local_value = new_global_counter;
                *local_delta = 0;
                return; // jump out of the loop
            }
            Payload::Error { code, text } => {
                // 22 => case `from` value mismatch; continue with the loop
                if code != 22 {
                    panic!("unexpected error code {}: {}", code, text)
                }
            }
            _ => panic!("unexpected response: {:?}", cas_resp),
        }
    }
}
