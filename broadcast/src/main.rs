use anyhow::{Context, Ok, Result};
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
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: String,
    },
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct Node {
    id: String,
    peer_ids: Vec<String>,
    peer_known_msgs: HashMap<String, HashSet<usize>>,
    broadcast_handles: HashMap<usize, usize>, // messageId: message
    msg_counter: usize,
    msg_read: HashSet<usize>,
}

impl Node {
    fn step(&mut self, req: Message) -> Vec<Message> {
        let payload = match req.body.payload.clone() {
            Payload::Init { node_id, node_ids } => {
                self.id = node_id;
                self.peer_ids = node_ids.clone();
                self.peer_known_msgs = node_ids
                    .into_iter()
                    .map(|nid| (nid, HashSet::new()))
                    .collect();

                Payload::InitOk
            }
            Payload::InitOk {} => panic!("unexpected message type InitOk received"),
            Payload::Echo { echo } => Payload::EchoOk { echo },
            Payload::EchoOk { .. } => {
                return vec![];
            }
            Payload::Generate => Payload::GenerateOk {
                id: format!("{}x{}", self.id, self.msg_counter),
            },
            Payload::GenerateOk { .. } => panic!("unexpected message type GenerateOk received"),
            Payload::Broadcast { message } => {
                self.msg_read.insert(message);
                Payload::BroadcastOk
            }
            Payload::BroadcastOk => {
                if let Some(&message) = self
                    .broadcast_handles
                    .get(&req.body.in_reply_to.expect("msg id not included"))
                {
                    let peer = self
                        .peer_known_msgs
                        .get_mut(&req.src)
                        .expect("peer hashmap is not properly initialized");
                    peer.insert(message);
                } else {
                    panic!("handle not found for id {:?}", req.body.in_reply_to)
                }
                self.broadcast_handles
                    .remove(&req.body.in_reply_to.unwrap());
                return vec![];
            }
            Payload::Read => Payload::ReadOk {
                messages: self.msg_read.clone().into_iter().collect(),
            },
            Payload::ReadOk { .. } => {
                return vec![];
            }
            Payload::Topology { topology } => {
                if let Some(neighbours) = topology.get(&self.id) {
                    self.peer_ids = neighbours.clone();
                }
                Payload::TopologyOk
            }
            Payload::TopologyOk => {
                return vec![];
            }
        };

        let mut resp = vec![];
        if let Payload::Broadcast { message } = req.body.payload {
            for peer_id in &self.peer_ids {
                let peer = self
                    .peer_known_msgs
                    .get(peer_id)
                    .expect("peer known msgs is not properly init");
                if peer.contains(&message) {
                    continue;
                }
                resp.push(Message {
                    src: req.dest.clone(),
                    dest: peer_id.clone(),
                    body: MessageBody {
                        msg_id: Some(self.msg_counter),
                        in_reply_to: None,
                        payload: Payload::Broadcast { message },
                    },
                });
                self.broadcast_handles.insert(self.msg_counter, message);
                self.msg_counter += 1;
            }
        }

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
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut node = Node {
        id: "uninitialized-node-id".to_string(),
        msg_counter: 0,
        msg_read: HashSet::new(),
        peer_ids: Vec::new(),
        peer_known_msgs: HashMap::new(),
        broadcast_handles: HashMap::new(),
    };

    for line in stdin.lines() {
        let line_str = line.context("read line")?;
        // dbg!("incomming message:", &line_str);
        let req: Message =
            serde_json::from_str(&line_str).context("serde deserialize msg from STDIN")?;
        // dbg!("incomming message:", req.clone(), line_str);
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
