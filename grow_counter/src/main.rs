mod counter;
mod node_manager;
mod response_collector;

use anyhow::Result;
use core::panic;
use std::sync::Mutex;
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::Arc,
};
use tokio::sync::mpsc::*;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
struct MessageBody {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    payload: Payload,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Add {
        delta: usize,
    },
    AddOk,
    Read {
        key: Option<String>,
    },
    ReadOk {
        value: usize,
    },
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },

    Cas {
        key: String,
        from: usize,
        to: usize,
    },
    CasOk,
    Error {
        code: usize,
        text: String,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
}

const GLOBAL_COUNTER_KEY: &str = "my-counter";
const SEQ_KV_SVC: &str = "seq-kv";

async fn process_msg(
    msg: Message,
    response_tx: Sender<response_collector::Command>,
    node_tx: Sender<node_manager::Commands>,
    msg_cnt: Arc<Mutex<usize>>,
    counter_tx: Sender<counter::Commands>,
) {
    // handle response
    if let Some(in_reply_to) = msg.body.in_reply_to {
        eprintln!("handling response...");
        response_tx
            .send(response_collector::Command::Put {
                msg_id: in_reply_to,
                msg,
            })
            .await
            .unwrap();
        return;
    }

    eprintln!("handling request");

    let mut nid = node_manager::get_node_id(node_tx.clone()).await;

    eprintln!("node id received {}", nid.clone());

    // handle request
    let payload = match msg.body.payload.clone() {
        Payload::Init { node_id, node_ids } => {
            nid = node_id.clone();
            node_tx
                .send(node_manager::Commands::InitNode {
                    id: nid.clone(),
                    peer_ids: node_ids,
                })
                .await
                .unwrap();

            eprintln!("init global counter...");
            let write_id = {
                let id = new_msg_id(msg_cnt.clone());
                send_msg(Message {
                    src: nid.clone(),
                    dest: SEQ_KV_SVC.to_string(),
                    body: MessageBody {
                        msg_id: Some(id),
                        in_reply_to: None,
                        payload: Payload::Write {
                            key: GLOBAL_COUNTER_KEY.to_string(),
                            value: 0,
                        },
                    },
                });
                id
            };

            eprintln!("awaiting init global counter msg with id {}", write_id);

            let write_resp = response_collector::take(response_tx.clone(), write_id).await;
            match write_resp.body.payload {
                Payload::WriteOk => {
                    eprintln!("global counter initialized!");
                }
                _ => panic!("couldn't init global counter"),
            }

            counter_tx
                .send(counter::Commands::Init {
                    node_id: nid.clone(),
                })
                .await
                .unwrap();

            Payload::InitOk
        }
        Payload::Topology { .. } => Payload::TopologyOk,
        Payload::TopologyOk => {
            return;
        }
        Payload::Echo { echo } => Payload::EchoOk { echo },
        Payload::EchoOk { .. } => {
            return;
        }
        Payload::Add { delta } => {
            eprintln!("handling add delta {}", delta);
            counter_tx
                .send(counter::Commands::Add { delta })
                .await
                .unwrap();
            Payload::AddOk
        }
        Payload::Read { .. } => {
            eprintln!("handling read...");

            let (tx, rx) = tokio::sync::oneshot::channel();

            counter_tx
                .send(counter::Commands::Read { resp: tx })
                .await
                .unwrap();

            eprintln!("waiting for resp...");
            let value = rx.await.unwrap();
            eprintln!("read ok!");
            Payload::ReadOk { value }
        }
        Payload::InitOk {} => panic!("unexpected message type InitOk received"),
        Payload::AddOk => panic!("unexpected branch of code"),
        Payload::ReadOk { .. } => panic!("unexpected branch of code"),
        Payload::Cas { .. } => panic!("unexpected branch of code"),
        Payload::CasOk => panic!("unexpected branch of code"),
        Payload::Write { .. } => panic!("unexpected branch of code"),
        Payload::WriteOk { .. } => panic!("unexpected branch of code"),
        Payload::Error { .. } => panic!("unexpected branch of code"),
    };

    let resp_id = new_msg_id(msg_cnt.clone());
    send_msg(Message {
        src: msg.dest,
        dest: msg.src,
        body: MessageBody {
            msg_id: Some(resp_id),
            in_reply_to: msg.body.msg_id,
            payload,
        },
    });
}

fn new_msg_id(cnt: Arc<std::sync::Mutex<usize>>) -> usize {
    let mut msg_cnt = cnt.lock().unwrap();
    let tmp = *msg_cnt;
    *msg_cnt += 1;
    tmp
}

#[tokio::main]
async fn main() -> Result<()> {
    let response_collector = response_collector::ResponseCollector::new();
    let msg_cnt = Arc::new(std::sync::Mutex::new(1));
    let node_manager = node_manager::NodeManager::new();
    let counter = counter::Counter::new(msg_cnt.clone(), response_collector.sender());

    loop {
        let resp_tx = response_collector.sender();
        let node_tx = node_manager.sender();
        let msg_cnt_arc = msg_cnt.clone();
        let counter_tx = counter.sender();
        // blocking receive
        let msg = receive_msg();
        eprintln!("received msg: {:?}", msg);
        tokio::spawn(async move {
            eprintln!("task was spawned");
            process_msg(msg, resp_tx, node_tx, msg_cnt_arc, counter_tx).await;
        });
    }
}

fn receive_msg() -> Message {
    let mut stdin = io::stdin().lock();
    let mut line = String::new();
    stdin.read_line(&mut line).expect("read line");
    let msg: Message = serde_json::from_str(&line).expect("parsing received msg");
    msg
}

fn send_msg(msg: Message) {
    let mut stdout = io::stdout().lock();
    let serialized_msg = serde_json::to_string(&msg).expect("serialize Message") + "\n";
    stdout.write_all(serialized_msg.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

fn send_global_counter_read(msg_id: usize, src: String) {
    let read_kv_msg = Message {
        src: src,
        dest: SEQ_KV_SVC.to_string(),
        body: MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: None,
            payload: Payload::Read {
                key: Some(GLOBAL_COUNTER_KEY.to_string()),
            },
        },
    };
    eprintln!("sending msg to seq-kv {:?}", read_kv_msg);
    send_msg(read_kv_msg);
}
