use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::mpsc,
};

use crate::message::Payload;

mod message;

fn main() {
    let node_tx = start_node_manager();

    eprintln!("starting the loop...");
    loop {
        let msg = receive_msg();
        process_msg(msg, node_tx.clone());
    }
}

fn process_msg(req: message::Message, node_tx: mpsc::Sender<NodeCommand>) {
    use message::Payload::*;
    match req.body.payload.clone() {
        Init { node_id, node_ids } => {
            eprintln!("initializing node..");
            node_init(node_tx.clone(), node_id);
            send_reply(req, node_tx.clone(), message::Payload::InitOk);
        }
        Topology { topology } => todo!(),
        // Error { code, text } => todo!(),
        Send { key, msg } => {
            let offset = append_msg(node_tx.clone(), key, msg);
            send_reply(req, node_tx.clone(), message::Payload::SendOk { offset });
        }
        // SendOk { offset } => todo!(),
        Poll { offsets } => {
            let msgs = poll_logs(node_tx.clone(), offsets);
            send_reply(req, node_tx.clone(), message::Payload::PollOk { msgs });
        }
        // PollOk { msgs } => todo!(),
        CommitOffsets { offsets } => {
            commit_offsets(node_tx.clone(), req.src.clone(), offsets);
            send_reply(req, node_tx.clone(), message::Payload::CommitOffsetsOk);
        }
        // CommitOffsetsOk => todo!(),
        ListCommitedOffsets { keys } => {
            let offsets = list_commited_offsets(node_tx.clone(), req.src.clone(), keys);
            send_reply(
                req,
                node_tx.clone(),
                message::Payload::ListCommitedOffsetsOk { offsets },
            );
        }
        // ListCommitedOffsetsOk { offsets } => todo!(),
        Echo { echo } => {
            eprintln!("hadnling echo..");
            send_reply(req, node_tx.clone(), message::Payload::EchoOk { echo });
        }
        _ => panic!("unexpected branch of code"),
    }
}

fn receive_msg() -> message::Message {
    let mut stdin = io::stdin().lock();
    let mut line = String::new();
    stdin.read_line(&mut line).expect("read line");
    let msg: message::Message = serde_json::from_str(&line).expect("parsing received msg");
    msg
}

fn send_msg(msg: message::Message) {
    let mut stdout = io::stdout().lock();
    let serialized_msg = serde_json::to_string(&msg).expect("serialize Message") + "\n";
    stdout.write_all(serialized_msg.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

fn send_reply(
    reply_to: message::Message,
    node_tx: mpsc::Sender<NodeCommand>,
    payload: message::Payload,
) {
    let msg_id = next_msg_id(node_tx);
    send_msg(message::Message {
        src: reply_to.dest,
        dest: reply_to.src,
        body: message::MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: reply_to.body.msg_id,
            payload,
        },
    });
}

fn next_msg_id(node_tx: mpsc::Sender<NodeCommand>) -> usize {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::NextMsgId { sender: tx })
        .expect("send next msg id via channel");
    rx.recv().expect("receive msg_id")
}

fn node_init(node_tx: mpsc::Sender<NodeCommand>, node_id: String) {
    node_tx
        .send(NodeCommand::Init { id: node_id })
        .expect("send node init cmd");
}

fn append_msg(node_tx: mpsc::Sender<NodeCommand>, key: String, msg: usize) -> usize {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::LogAppend {
            key,
            msg,
            sender_offset: tx,
        })
        .expect("send log append");
    rx.recv().expect("offset receive")
}

fn poll_logs(
    node_tx: mpsc::Sender<NodeCommand>,
    keys_offsets: HashMap<String, usize>,
) -> HashMap<String, Vec<Vec<usize>>> {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::Poll {
            keys_offsets,
            sender_msgs: tx,
        })
        .expect("send poll");
    rx.recv().expect("poll receive")
}

fn commit_offsets(
    node_tx: mpsc::Sender<NodeCommand>,
    src: String,
    offsets: HashMap<String, usize>,
) {
    node_tx
        .send(NodeCommand::CommitOffsets {
            client_id: src,
            offsets,
        })
        .expect("send commit offsets")
}

fn list_commited_offsets(
    node_tx: mpsc::Sender<NodeCommand>,
    src: String,
    keys: Vec<String>,
) -> HashMap<String, usize> {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::ListCommitedOffsets {
            client_id: src,
            keys,
            sender_offsets: tx,
        })
        .expect("send commit offsets");
    rx.recv().expect("recv commited offsets from channel")
}

enum NodeCommand {
    Init {
        id: String,
    },
    GetNodeId {
        sender: mpsc::Sender<String>,
    },
    NextMsgId {
        sender: mpsc::Sender<usize>,
    },
    LogAppend {
        key: String,
        msg: usize,
        sender_offset: mpsc::Sender<usize>,
    },
    Poll {
        keys_offsets: HashMap<String, usize>,
        sender_msgs: mpsc::Sender<HashMap<String, Vec<Vec<usize>>>>,
    },
    CommitOffsets {
        client_id: String,
        offsets: HashMap<String, usize>,
    },
    ListCommitedOffsets {
        client_id: String,
        keys: Vec<String>,
        sender_offsets: mpsc::Sender<HashMap<String, usize>>,
    },
}

struct Node {
    id: String,
    logs: HashMap<String, Vec<usize>>,
    msg_id_cnt: usize,
    client_offsets: HashMap<String, HashMap<String, usize>>,
}

impl Node {
    fn pull_logs(&self, keys_offsets: HashMap<String, usize>) -> HashMap<String, Vec<Vec<usize>>> {
        let mut out = HashMap::new();
        for (req_key, start_offset) in keys_offsets.into_iter() {
            if let Some(logs) = self.logs.get(&req_key) {
                let mut key_vec: Vec<Vec<usize>> = Vec::new();
                for (i, &msg) in logs[start_offset..].into_iter().enumerate() {
                    key_vec.push(vec![i + start_offset, msg]);
                }
                out.insert(req_key, key_vec);
            }
        }
        out
    }

    fn client_offsets(&self, client_id: String, req_keys: Vec<String>) -> HashMap<String, usize> {
        self.client_offsets
            .get(&client_id)
            .expect("client offsets for given key")
            .into_iter()
            .filter(|(k, _)| req_keys.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

fn start_node_manager() -> mpsc::Sender<NodeCommand> {
    let (tx, rx) = mpsc::channel();
    let mut node = Node {
        id: String::new(),
        logs: HashMap::new(),
        msg_id_cnt: 0,
        client_offsets: HashMap::new(),
    };

    std::thread::spawn(move || {
        use NodeCommand::*;
        loop {
            match rx.recv().unwrap() {
                Init { id } => node.id = id,
                GetNodeId { sender } => sender
                    .send(node.id.clone())
                    .expect("send node id through channel"),
                NextMsgId { sender } => {
                    sender.send(node.msg_id_cnt.clone()).unwrap();
                    node.msg_id_cnt += 1;
                }
                LogAppend {
                    key,
                    msg,
                    sender_offset,
                } => {
                    if let Some(log) = node.logs.get_mut(&key) {
                        log.push(msg);
                    } else {
                        node.logs.insert(key.clone(), vec![msg]);
                    }
                    sender_offset
                        .send(node.logs.get(&key).unwrap().len() - 1)
                        .expect("send offset via channel")
                }
                Poll {
                    keys_offsets,
                    sender_msgs,
                } => sender_msgs
                    .send(node.pull_logs(keys_offsets))
                    .expect("send offset messages"),
                CommitOffsets { client_id, offsets } => {
                    node.client_offsets.insert(client_id, offsets);
                }
                ListCommitedOffsets {
                    client_id,
                    keys,
                    sender_offsets,
                } => {
                    let client_offsets = node.client_offsets(client_id, keys);
                    sender_offsets
                        .send(client_offsets)
                        .expect("client offsets sent");
                }
            }
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn node_pull_logs() {
        let node = Node {
            id: "id".to_string(),
            logs: HashMap::from([
                ("k1".to_string(), vec![1, 2, 3, 4]),
                ("k2".to_string(), vec![10, 20, 30, 40]),
                ("k3".to_string(), vec![100, 200, 300, 400]),
            ]),
            msg_id_cnt: 0,
            client_offsets: HashMap::new(),
        };

        assert_eq!(
            HashMap::from([
                ("k1".to_string(), vec![vec![2, 3], vec![3, 4]]),
                (
                    "k2".to_string(),
                    vec![vec![1, 20], vec![2, 30], vec![3, 40]]
                ),
                (
                    "k3".to_string(),
                    vec![vec![0, 100], vec![1, 200], vec![2, 300], vec![3, 400]]
                ),
            ]),
            node.pull_logs(HashMap::from([
                ("k1".to_string(), 2),
                ("k2".to_string(), 1),
                ("k3".to_string(), 0),
            ]))
        );
    }

    #[test]
    fn node_client_offsets() {
        let node = Node {
            id: "id".to_string(),
            logs: HashMap::new(),
            msg_id_cnt: 0,
            client_offsets: HashMap::from([
                (
                    "c1".to_string(),
                    HashMap::from([
                        ("k1".to_string(), 5),
                        ("k2".to_string(), 10),
                        ("k3".to_string(), 15),
                    ]),
                ),
                (
                    "c2".to_string(),
                    HashMap::from([
                        ("k1".to_string(), 50),
                        ("k2".to_string(), 100),
                        ("k3".to_string(), 150),
                    ]),
                ),
            ]),
        };

        assert_eq!(
            HashMap::from([("k2".to_string(), 10), ("k3".to_string(), 15)]),
            node.client_offsets("c1".to_string(), vec!["k2".to_string(), "k3".to_string()])
        );
        assert_eq!(
            HashMap::from([("k1".to_string(), 50), ("k3".to_string(), 150)]),
            node.client_offsets("c2".to_string(), vec!["k1".to_string(), "k3".to_string()])
        );
    }
}
