use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::mpsc,
};

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
        Poll { offsets } => {}
        // PollOk { msgs } => todo!(),
        CommitOffsets { offsets } => todo!(),
        // CommitOffsetsOk => todo!(),
        ListCommitedOffsets { keys } => todo!(),
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
}

struct Node {
    id: String,
    logs: HashMap<String, Vec<usize>>,
    msg_id_cnt: usize,
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
}

fn start_node_manager() -> mpsc::Sender<NodeCommand> {
    let (tx, rx) = mpsc::channel();
    let mut node = Node {
        id: String::new(),
        logs: HashMap::new(),
        msg_id_cnt: 0,
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
}
