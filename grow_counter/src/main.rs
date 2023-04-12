mod seqkv;
use anyhow::{Context, Ok, Result};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::{
    collections::{HashMap, HashSet},
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
    // Add {
    //     delta: usize,
    // },
    // AddOk,
    // Read,
    // ReadOk {
    //     value: usize,
    // },
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct Node {
    id: String,
    peer_ids: Vec<String>,
    msg_counter: usize,
}

impl Node {
    fn step(&mut self, req: Message) -> Vec<Message> {
        let payload = match req.body.payload.clone() {
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
                return vec![];
            }
            Payload::Echo { echo } => Payload::EchoOk { echo },
            Payload::EchoOk { .. } => {
                return vec![];
            }
        };

        let mut resp = vec![];
        resp.push(Message {
            src: req.dest,
            dest: req.src,
            body: MessageBody {
                msg_id: Some(self.msg_counter),
                in_reply_to: req.body.msg_id,
                payload,
            },
        });

        self.msg_counter += 1;
        resp
    }
}

fn main() -> Result<()> {
    // let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut node = Node {
        id: "uninitialized-node-id".to_string(),
        msg_counter: 0,
        peer_ids: Vec::new(),
    };

    let (tx, rx) = mpsc::sync_channel(1);
    let _router = Router::new(tx);

    for req in rx {
        let responses = node.step(req);
        for resp in responses {
            let serialized_msg = serde_json::to_string(&resp).context("serialize Message")? + "\n";
            // dbg!(resp, serialized_msg.clone());
            stdout.write_all(serialized_msg.as_bytes())?;
            stdout.flush()?;
        }
    }

    Ok(())
}
struct Router {
    msg_id_handles: Arc<RwLock<HashMap<usize, mpsc::SyncSender<Message>>>>,
}

impl Router {
    fn new(req_chan: mpsc::SyncSender<Message>) -> Self {
        let s = Self {
            msg_id_handles: Arc::new(RwLock::new(HashMap::new())),
        };

        let handles = s.msg_id_handles.clone();

        thread::spawn(move || {
            let stdin = io::stdin().lock();
            for line in stdin.lines() {
                let msg: Message = serde_json::from_str(&line.unwrap()).unwrap();
                if let Some(in_reply_to) = msg.body.in_reply_to {
                    // A) response to my request
                    // A1) there is no registered handle -> NOOP
                    // A2) notify registered handle
                    let mut h = handles.write().unwrap();
                    if let Some(handle) = h.get(&in_reply_to) {
                        // notify registered handle
                        handle.send(msg).unwrap();
                        // remove the handle
                        h.remove(&in_reply_to);
                    } else {
                        // there is no registered handle -> NOOP
                    }
                } else {
                    // B) incoming request
                    // - notify system
                    req_chan.send(msg).unwrap();
                }
            }
        });

        s
    }

    fn register_msg_id_handle(&self, msg_id: usize, sender: mpsc::SyncSender<Message>) {
        let mut handles = self.msg_id_handles.write().unwrap();
        handles.insert(msg_id, sender);
    }
}
