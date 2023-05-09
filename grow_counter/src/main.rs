use anyhow::Result;
use core::panic;
use tokio::sync::Mutex;

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

struct Node {
    id: String,
    peer_ids: Vec<String>,
    msg_counter: usize,
    response_collector: ResponseCollector,
}

const GLOBAL_COUNTER_KEY: &str = "my-counter";
const SEQ_KV_SVC: &str = "seq-kv";

impl Node {
    fn new() -> Self {
        Self {
            id: "uninitialized-node-id".to_string(),
            msg_counter: 0,
            peer_ids: Vec::new(),
            response_collector: ResponseCollector::new(),
        }
    }
    async fn step(&mut self, msg: Message) {
        // handle response
        if let Some(in_reply_to) = msg.body.in_reply_to {
            eprintln!("handling resposne...");
            self.response_collector.put_response(in_reply_to, msg);
            return;
        }

        eprintln!("handling request");

        // handle request
        let payload = match msg.body.payload.clone() {
            Payload::Write { .. } => panic!("unexpected branch of code"),
            Payload::WriteOk { .. } => panic!("unexpected branch of code"),
            Payload::Error { .. } => panic!("unexpected branch of code"),
            Payload::Init { node_id, node_ids } => {
                self.id = node_id;
                self.peer_ids = node_ids.clone();

                eprintln!("init global counter...");
                let write_id = {
                    let id = self.get_counter_and_increase();
                    send_msg(Message {
                        src: self.id.clone(),
                        dest: SEQ_KV_SVC.to_string(),
                        body: MessageBody {
                            msg_id: Some(id),
                            in_reply_to: None,
                            payload: Payload::Write {
                                key: SEQ_KV_SVC.to_string(),
                                value: 0,
                            },
                        },
                    });
                    id
                };

                eprintln!("awaiting init global counter msg with id {}", write_id);

                let write_resp = self.response_collector.take(write_id).await;
                match write_resp.body.payload {
                    Payload::WriteOk => {
                        eprintln!("global counter initialized!");
                    }
                    _ => panic!("couldn't init global counter"),
                }
                Payload::InitOk
            }
            Payload::InitOk {} => panic!("unexpected message type InitOk received"),
            Payload::Topology { topology } => {
                if let Some(neighbours) = topology.get(&self.id) {
                    self.peer_ids = neighbours.clone();
                }
                Payload::TopologyOk
            }
            Payload::TopologyOk => {
                return;
            }
            Payload::Echo { echo } => Payload::EchoOk { echo },
            Payload::EchoOk { .. } => {
                return;
            }
            Payload::Add { delta } => {
                eprintln!("handling add delta {}", delta);

                let read_id = self.get_counter_and_increase();
                send_global_counter_read(read_id, self.id.clone());

                eprintln!("awaiting response for msg id {}", read_id);

                let read_resp = self.response_collector.take(read_id).await;
                let global_counter = match read_resp.body.payload {
                    Payload::ReadOk { value } => value,
                    _ => panic!("unexpected response: {:?}", read_resp),
                };

                eprintln!("received global counter value {}", global_counter);

                let cas_id = {
                    let id = self.get_counter_and_increase();
                    send_msg(Message {
                        src: self.id.clone(),
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

                let cas_resp = self.response_collector.take(cas_id).await;
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

                let read_id = self.get_counter_and_increase();
                send_global_counter_read(read_id, self.id.clone());

                eprintln!("awaiting read response {}", read_id);
                let read_resp = self.response_collector.take(read_id).await;
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

        let resp_id = self.get_counter_and_increase();
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

    fn get_counter_and_increase(&mut self) -> usize {
        let tmp = self.msg_counter;
        self.msg_counter += 1;
        tmp
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let node = Arc::new(Mutex::new(Node::new()));

    loop {
        let node = node.clone();

        // blocking receive
        let msg = receive_msg();
        eprintln!("received msg: {:?}", msg);
        tokio::spawn(async move {
            eprintln!("task was spawned");
            let mut guard = node.lock().await;
            guard.step(msg).await;
        });
    }
}
// what you should do then is make step spawn a task everytime it's done reading a message, and make that task handle that message.

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
    stdout.flush();
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

struct ResponseCollector {
    responses: HashMap<usize, Message>,
}

impl ResponseCollector {
    fn new() -> Self {
        Self {
            responses: HashMap::new(),
        }
    }

    fn put_response(&mut self, msg_id: usize, msg: Message) {
        self.responses.insert(msg_id, msg);
    }

    async fn take(&mut self, msg_id: usize) -> Message {
        loop {
            if let Some(msg) = self.responses.remove_entry(&msg_id) {
                return msg.1;
            }
            tokio::task::yield_now().await;
        }
    }
}
