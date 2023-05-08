use anyhow::{Context, Result};
use core::panic;
use std::sync::{mpsc, Arc};
use std::thread;
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
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
}

struct Node<'a> {
    id: String,
    peer_ids: Vec<String>,
    msg_counter: usize,
    msg_receiver: &'a MsgReceiver,
    msg_sender: &'a MsgSender,
    response_collector: ResponseCollector,
}

const GLOBAL_COUNTER_KEY: &str = "my-counter";
const SEQ_KV_SVC: &str = "seq-kv";

impl<'a> Node<'a> {
    async fn step(&mut self) {
        // blocking receive
        let msg = self.msg_receiver.receive();

        eprintln!("received msg: {:?}", msg);

        // handle response
        if let Some(in_reply_to) = msg.body.in_reply_to {
            eprintln!("hadnling resposne...");
            self.response_collector.put_response(in_reply_to, msg);
            return;
        }

        eprintln!("handling request");

        // handle request
        let payload = match msg.body.payload.clone() {
            Payload::Error { .. } => panic!("unexpected branch of code"),
            Payload::Init { node_id, node_ids } => {
                self.id = node_id;
                self.peer_ids = node_ids.clone();

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
                let read_id = {
                    let r_id = self.get_counter_and_increase();
                    let read_kv_msg = Message {
                        src: self.id.clone(),
                        dest: SEQ_KV_SVC.to_string(),
                        body: MessageBody {
                            msg_id: Some(r_id),
                            in_reply_to: None,
                            payload: Payload::Read {
                                key: Some(GLOBAL_COUNTER_KEY.to_string()),
                            },
                        },
                    };
                    eprintln!("sending msg to seq-kv {:?}", read_kv_msg);
                    self.msg_sender.send(read_kv_msg);
                    r_id
                };

                eprintln!("awaiting response for msg id {}", read_id);

                let read_resp = self.response_collector.take(read_id).await;
                let global_counter = match read_resp.body.payload {
                    Payload::ReadOk { value } => value,
                    _ => panic!("unexpected response: {:?}", read_resp),
                };

                eprintln!("received global counter value {}", global_counter);

                let cas_id = {
                    let id = self.get_counter_and_increase();
                    self.msg_sender.send(Message {
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
                let read_id = {
                    let r_id = self.get_counter_and_increase();
                    let read_msg = Message {
                        src: self.id.clone(),
                        dest: SEQ_KV_SVC.to_string(),
                        body: MessageBody {
                            msg_id: Some(r_id),
                            in_reply_to: None,
                            payload: Payload::Read {
                                key: Some(GLOBAL_COUNTER_KEY.to_string()),
                            },
                        },
                    };
                    eprintln!("sending msg to seqkv {:?}", read_msg);
                    self.msg_sender.send(read_msg);
                    r_id
                };

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
        self.msg_sender.send(Message {
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
    let node = Arc::new(&mut Node {
        id: "uninitialized-node-id".to_string(),
        msg_counter: 0,
        peer_ids: Vec::new(),
        msg_receiver: &MsgReceiver::new(),
        msg_sender: &MsgSender::new(),
        response_collector: ResponseCollector::new(),
    });

    loop {
        tokio::spawn(async move {
            eprintln!("step!");
            node.step().await;
        });
    }
}
struct MsgReceiver {
    receiver_ch: Arc<mpsc::Receiver<Message>>,
}

impl MsgReceiver {
    fn new() -> Self {
        let (reciever_in, reciever_out) = mpsc::sync_channel(0);

        thread::spawn(move || {
            let stdin = io::stdin().lock();
            for line in stdin.lines() {
                let msg: Message =
                    serde_json::from_str(&line.unwrap()).expect("parsing received msg");
                reciever_in.send(msg).expect("send parsed msg");
            }
        });

        Self {
            receiver_ch: Arc::new(reciever_out),
        }
    }

    fn receive(&self) -> Message {
        self.receiver_ch
            .recv()
            .expect("receive msg from stdin wrapper")
    }
}

struct MsgSender {
    stdout_sender: mpsc::SyncSender<Message>,
}

impl MsgSender {
    fn new() -> Self {
        let (stdout_sender, stdout_receiver) = mpsc::sync_channel(0);
        thread::spawn(move || {
            let mut stdout = io::stdout().lock();
            loop {
                let msg: Message = stdout_receiver.recv().expect("recv msg for writing");
                let serialized_msg = serde_json::to_string(&msg)
                    .context("serialize Message")
                    .unwrap()
                    + "\n";
                stdout.write_all(serialized_msg.as_bytes()).unwrap();
                stdout.flush().unwrap();
            }
        });
        Self { stdout_sender }
    }

    fn send(&self, msg: Message) {
        self.stdout_sender
            .send(msg)
            .expect("send_and_forget send msg");
    }
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
