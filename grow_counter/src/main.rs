use anyhow::Result;
use core::panic;
use std::sync::Mutex;

use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::Arc,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
struct Message {
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
    response_tx: tokio::sync::mpsc::Sender<RespColCommand>,
    node_tx: tokio::sync::mpsc::Sender<NodeCommands>,
    msg_cnt: Arc<Mutex<usize>>,
) {
    // handle response
    if let Some(in_reply_to) = msg.body.in_reply_to {
        eprintln!("handling response...");
        response_tx
            .send(RespColCommand::Put {
                msg_id: in_reply_to,
                msg,
            })
            .await
            .unwrap();
        return;
    }

    eprintln!("handling request");

    let mut nid = get_node_id(node_tx.clone()).await;

    eprintln!("node id received {}", nid.clone());

    // handle request
    let payload = match msg.body.payload.clone() {
        Payload::Write { .. } => panic!("unexpected branch of code"),
        Payload::WriteOk { .. } => panic!("unexpected branch of code"),
        Payload::Error { .. } => panic!("unexpected branch of code"),
        Payload::Init { node_id, node_ids } => {
            nid = node_id.clone();
            node_tx
                .send(NodeCommands::InitNode {
                    id: nid.clone(),
                    peer_ids: node_ids,
                })
                .await
                .unwrap();

            eprintln!("init global counter...");
            let write_id = {
                let id = get_counter_and_increase(msg_cnt.clone());
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

            let write_resp = take(response_tx.clone(), write_id).await;
            match write_resp.body.payload {
                Payload::WriteOk => {
                    eprintln!("global counter initialized!");
                }
                _ => panic!("couldn't init global counter"),
            }
            Payload::InitOk
        }
        Payload::InitOk {} => panic!("unexpected message type InitOk received"),
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

            let read_id = get_counter_and_increase(msg_cnt.clone());
            send_global_counter_read(read_id, nid.clone());

            eprintln!("awaiting response for msg id {}", read_id);

            let read_resp = take(response_tx.clone(), read_id).await;
            let global_counter = match read_resp.body.payload {
                Payload::ReadOk { value } => value,
                _ => panic!("unexpected response: {:?}", read_resp),
            };

            eprintln!("received global counter value {}", global_counter);

            let cas_id = {
                let id = get_counter_and_increase(msg_cnt.clone());
                send_msg(Message {
                    src: nid.clone(),
                    dest: SEQ_KV_SVC.to_string(),
                    body: MessageBody {
                        msg_id: Some(id),
                        in_reply_to: None,
                        payload: Payload::Cas {
                            key: GLOBAL_COUNTER_KEY.to_string(),
                            from: global_counter,
                            to: global_counter + delta,
                        },
                    },
                });
                id
            };

            let cas_resp = take(response_tx.clone(), cas_id).await;
            match cas_resp.body.payload {
                Payload::CasOk => Payload::AddOk,
                _ => panic!("unexpected response: {:?}", cas_resp),
            }
        }
        Payload::AddOk => {
            panic!("unexpected branch of code")
        }
        Payload::Read { .. } => {
            eprintln!("handling read...");

            let read_id = get_counter_and_increase(msg_cnt.clone());
            send_global_counter_read(read_id, nid.clone());

            eprintln!("awaiting read response {}", read_id);
            let read_resp = take(response_tx.clone(), read_id).await;
            let global_counter = match read_resp.body.payload {
                Payload::ReadOk { value } => value,
                _ => panic!("unexpected response: {:?}", read_resp),
            };
            eprintln!("received global counter value {}", global_counter);
            Payload::ReadOk {
                value: global_counter,
            }
        }
        Payload::ReadOk { .. } => panic!("unexpected branch of code"),
        Payload::Cas { .. } => panic!("unexpected branch of code"),
        Payload::CasOk => panic!("unexpected branch of code"),
    };

    let resp_id = get_counter_and_increase(msg_cnt.clone());
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

fn get_counter_and_increase(cnt: Arc<std::sync::Mutex<usize>>) -> usize {
    let mut msg_cnt = cnt.lock().unwrap();
    let tmp = *msg_cnt;
    *msg_cnt += 1;
    tmp
}

#[tokio::main]
async fn main() -> Result<()> {
    let response_collector = ResponseCollector::new();
    let msg_cnt = Arc::new(std::sync::Mutex::new(1));
    let node_manager = NodeManager::new();

    loop {
        let resp_tx = response_collector.sender();
        let node_tx = node_manager.sender();
        let msg_cnt_arc = msg_cnt.clone();
        // blocking receive
        let msg = receive_msg();
        eprintln!("received msg: {:?}", msg);
        tokio::spawn(async move {
            eprintln!("task was spawned");
            process_msg(msg, resp_tx, node_tx, msg_cnt_arc).await;
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

async fn take(tx: tokio::sync::mpsc::Sender<RespColCommand>, msg_id: usize) -> Message {
    loop {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        tx.send(RespColCommand::GetAndRemove {
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
enum RespColCommand {
    Put {
        msg_id: usize,
        msg: Message,
    },
    GetAndRemove {
        msg_id: usize,
        resp: tokio::sync::oneshot::Sender<Option<Message>>,
    },
}

struct ResponseCollector {
    tx: tokio::sync::mpsc::Sender<RespColCommand>,
}

impl ResponseCollector {
    fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);

        tokio::spawn(async move {
            let mut responses = HashMap::new();

            while let Some(cmd) = rx.recv().await {
                use RespColCommand::*;

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

    fn sender(&self) -> tokio::sync::mpsc::Sender<RespColCommand> {
        self.tx.clone()
    }
}

#[derive(Debug)]
enum NodeCommands {
    InitNode {
        id: String,
        peer_ids: Vec<String>,
    },
    GetID {
        resp: tokio::sync::oneshot::Sender<Option<String>>,
    },
}

async fn get_node_id(tx: tokio::sync::mpsc::Sender<NodeCommands>) -> String {
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

    tx.send(NodeCommands::GetID { resp: resp_tx })
        .await
        .unwrap();

    if let Some(msg) = resp_rx.await.unwrap() {
        return msg;
    } else {
        return String::new();
    }
}

struct NodeManager {
    tx: tokio::sync::mpsc::Sender<NodeCommands>,
}

impl NodeManager {
    fn new() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);

        tokio::spawn(async move {
            let mut node_id: Option<String> = None;
            let mut peers: Option<Vec<String>> = None;

            while let Some(cmd) = rx.recv().await {
                use NodeCommands::*;

                match cmd {
                    InitNode { id, peer_ids } => {
                        node_id = Some(id);
                        peers = Some(peer_ids);
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

    fn sender(&self) -> tokio::sync::mpsc::Sender<NodeCommands> {
        self.tx.clone()
    }
}

// 2 options to make it faster
// 1) synchronize with kv store only once in x seconds, or
// 2) use gossip instead to synchronize once in x seconds
