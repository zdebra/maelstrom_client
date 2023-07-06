use std::{
    collections::{HashMap, VecDeque},
    io::{self, BufRead, Write},
    sync::{mpsc, Arc, Mutex},
};

mod message;

fn main() {
    let resp_router = Arc::new(Mutex::new(ResponseRouter::new()));
    let node_tx = start_node_manager(Arc::clone(&resp_router));

    eprintln!("starting the loop...");
    loop {
        let msg = receive_msg();
        let node_tx = node_tx.clone();
        if let Some(in_reply_to) = msg.body.in_reply_to {
            resp_router
                .clone()
                .lock()
                .unwrap()
                .route(in_reply_to, msg)
                .unwrap();
        } else {
            std::thread::spawn(move || {
                process_msg(msg, node_tx);
            });
        }
    }
}

fn process_msg(req: message::Message, node_tx: mpsc::Sender<NodeCommand>) {
    use message::Payload::*;
    match req.body.payload.clone() {
        Init { node_id, node_ids } => {
            eprintln!("initializing node..");
            node_init(node_tx.clone(), node_id, node_ids);
            send_reply(req, node_tx.clone(), message::Payload::InitOk);
        }
        // Error { code, text } => todo!(),
        Send { key, msg } => {
            let offset = append_msg(node_tx.clone(), key, msg);
            send_reply(req, node_tx.clone(), message::Payload::SendOk { offset });
        }
        Poll { offsets } => {
            let msgs = poll_logs(node_tx.clone(), offsets);
            send_reply(req, node_tx.clone(), message::Payload::PollOk { msgs });
        }
        CommitOffsets { offsets } => {
            commit_offsets(node_tx.clone(), req.src.clone(), offsets);
            send_reply(req, node_tx.clone(), message::Payload::CommitOffsetsOk);
        }
        ListCommittedOffsets { keys } => {
            let offsets = list_committed_offsets(node_tx.clone(), req.src.clone(), keys);
            send_reply(
                req,
                node_tx.clone(),
                message::Payload::ListCommittedOffsetsOk { offsets },
            );
        }
        Echo { echo } => {
            eprintln!("hadnling echo..");
            send_reply(req, node_tx.clone(), message::Payload::EchoOk { echo });
        }
        Gossip {
            last_log_offset,
            last_client_offsets,
        } => {
            eprintln!("handling gossip");
            let diffs = get_diffs(node_tx.clone(), last_log_offset, last_client_offsets);
            send_reply(
                req,
                node_tx.clone(),
                message::Payload::GossipOk {
                    diff_logs: diffs.0,
                    diff_client_offsets: diffs.1,
                },
            )
        }
        GossipOk {
            diff_logs,
            diff_client_offsets,
        } => {
            eprintln!("handling response to previously sent requests for synchronization");
            apply_diffs(node_tx.clone(), diff_logs, diff_client_offsets);
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

fn node_init(node_tx: mpsc::Sender<NodeCommand>, node_id: String, neighbours: Vec<String>) {
    node_tx
        .send(NodeCommand::Init {
            id: node_id,
            neighbours: neighbours,
        })
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

fn list_committed_offsets(
    node_tx: mpsc::Sender<NodeCommand>,
    src: String,
    keys: Vec<String>,
) -> HashMap<String, usize> {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::ListCommittedOffsets {
            client_id: src,
            keys,
            sender_offsets: tx,
        })
        .expect("send commit offsets");
    rx.recv().expect("recv commited offsets from channel")
}

fn get_diffs(
    node_tx: mpsc::Sender<NodeCommand>,
    last_log_offset: HashMap<String, usize>, // key to offset
    last_client_offsets: HashMap<String, HashMap<String, usize>>, // client id to map of key to offset
) -> (
    Vec<message::LogDiff>,
    HashMap<String, HashMap<String, usize>>,
) {
    let (tx, rx) = mpsc::channel();
    node_tx
        .send(NodeCommand::InfraGetDiffs {
            last_log_offset,
            last_client_offsets,
            sender_diff: tx,
        })
        .expect("get diffs");
    rx.recv().expect("recv diffs from channel")
}

fn apply_diffs(
    node_tx: mpsc::Sender<NodeCommand>,
    diff_logs: Vec<message::LogDiff>,
    diff_client_offsets: HashMap<String, HashMap<String, usize>>,
) {
    node_tx.send(NodeCommand::InfraApplyDiffs {
        diff_logs,
        diff_client_offsets,
    });
}

enum NodeCommand {
    Init {
        id: String,
        neighbours: Vec<String>,
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
    ListCommittedOffsets {
        client_id: String,
        keys: Vec<String>,
        sender_offsets: mpsc::Sender<HashMap<String, usize>>,
    },
    InfraGetDiffs {
        last_log_offset: HashMap<String, usize>, // key to offset
        last_client_offsets: HashMap<String, HashMap<String, usize>>, // client id to map of key to offset
        sender_diff: mpsc::Sender<(
            Vec<message::LogDiff>,
            HashMap<String, HashMap<String, usize>>,
        )>,
    },
    InfraSyncNow,
    InfraApplyDiffs {
        diff_logs: Vec<message::LogDiff>,
        diff_client_offsets: HashMap<String, HashMap<String, usize>>,
    },
}

struct Node {
    id: String,
    logs: HashMap<String, Vec<usize>>, // gossip
    msg_id_cnt: usize,
    client_offsets: HashMap<String, HashMap<String, usize>>, // gossip
    neighbours: Vec<String>,
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
        if !self.client_offsets.contains_key(&client_id) {
            return HashMap::new();
        }
        self.client_offsets
            .get(&client_id)
            .expect("client offsets for given key")
            .into_iter()
            .filter(|(k, _)| req_keys.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn next_msg_id(&mut self) -> usize {
        let tmp = self.msg_id_cnt;
        self.msg_id_cnt += 1;
        tmp
    }

    fn next_n_msg_ids(&mut self, n: usize) -> Vec<usize> {
        (0..n).map(|_| self.next_msg_id()).collect()
    }

    fn get_node_id(&self) -> String {
        self.id.clone()
    }

    fn log_diffs(&self, last_log_offset: HashMap<String, usize>) -> Vec<message::LogDiff> {
        let mut diffs = Vec::new();
        for (key, local_logs) in &self.logs {
            let remote_offset = match last_log_offset.get(key) {
                Some(n) => *n,
                None => {
                    diffs.push(message::LogDiff {
                        key: key.to_string(),
                        messages: local_logs.to_vec(),
                        starting_offset: 0,
                    });
                    continue;
                }
            };
            if local_logs.len() <= remote_offset {
                continue;
            }
            let start_offset = remote_offset + 1;
            diffs.push(message::LogDiff {
                key: key.to_string(),
                messages: local_logs[start_offset..].to_vec(),
                starting_offset: start_offset,
            });
        }
        diffs.sort();
        diffs
    }

    fn client_offset_diffs(
        &self,
        last_client_offsets: HashMap<String, HashMap<String, usize>>,
    ) -> HashMap<String, HashMap<String, usize>> {
        let mut out = HashMap::new();
        for (client_id, key_offset) in &self.client_offsets {
            let mut diffs = HashMap::new();
            let empty_hm = HashMap::new();
            let remote_key_offset = match last_client_offsets.get(client_id) {
                Some(x) => x,
                None => &empty_hm,
            };
            for (key, local_offset) in key_offset {
                let remote_offset = match remote_key_offset.get(key) {
                    Some(n) => *n,
                    None => 0,
                };
                if remote_offset >= *local_offset {
                    continue;
                }
                diffs.insert(key.to_string(), *local_offset);
            }

            out.insert(client_id.to_string(), diffs);
        }
        out
    }

    fn request_updates(&mut self) {
        let mut last_log_offset = HashMap::new();
        for (key, messages) in &self.logs {
            last_log_offset.insert(key.to_string(), messages.len() - 1);
        }

        let mut allocted_msg_ids = VecDeque::from(self.next_n_msg_ids(self.neighbours.len()));
        for neighbour in &self.neighbours {
            send_msg(message::Message::new_request(
                self.get_node_id(),
                neighbour.clone(),
                allocted_msg_ids.pop_front().unwrap(),
                message::Payload::Gossip {
                    last_log_offset: last_log_offset.clone(),
                    last_client_offsets: self.client_offsets.clone(),
                },
            ))
        }
    }

    fn apply_diff_logs(&mut self, diff_logs: Vec<message::LogDiff>) {
        for diff_log in diff_logs {
            if let Some(log) = self.logs.get_mut(&diff_log.key) {
                // the log with given key is there, let's update the value
                let diff_size = diff_log.messages.len();
                let last_local_offset = log.len() - 1;
                if diff_log.starting_offset > last_local_offset + 1 {
                    panic!(
                        "incoming starting offset `{}` is bigger than local offset `{}` for key `{}`",
                        diff_log.starting_offset,
                        last_local_offset,
                        diff_log.key
                    );
                }
                for i in 0..diff_size {
                    let index = i + diff_log.starting_offset;
                    if index > log.len() - 1 {
                        log.push(diff_log.messages[i])
                    } else {
                        log[index] = diff_log.messages[i];
                    }
                }
            } else {
                // the log with given key is not present at all, let's create the record
                if diff_log.starting_offset != 0 {
                    panic!(
                        "starting offset is not zero while the record was not found for key `{}`",
                        diff_log.key
                    )
                }
                self.logs.insert(diff_log.key, diff_log.messages);
            }
        }
    }

    fn apply_diff_client_offsets(
        &mut self,
        diff_client_offsets: HashMap<String, HashMap<String, usize>>,
    ) {
        for (client, diff_offsets) in diff_client_offsets {
            if let Some(local_client) = self.client_offsets.get_mut(&client) {
                for (diff_key, diff_offset) in diff_offsets {
                    if let Some(local_client_offset) = local_client.get_mut(&diff_key) {
                        // update client's `diff_key` value
                        if diff_offset > *local_client_offset {
                            *local_client_offset = diff_offset;
                        }
                    } else {
                        // existing client doesn't heard about `diff_key`
                        local_client.insert(diff_key, diff_offset);
                    }
                }
            } else {
                // local client not found; let's create one
                self.client_offsets.insert(client, diff_offsets);
            }
        }
    }
}

const PREFIX_KEY_WRITE: &str = "W_";

fn start_node_manager(resp_router: Arc<Mutex<ResponseRouter>>) -> mpsc::Sender<NodeCommand> {
    let (tx, rx) = mpsc::channel();
    let mut node = Node {
        id: String::new(),
        logs: HashMap::new(),
        msg_id_cnt: 0,
        client_offsets: HashMap::new(),
        neighbours: Vec::new(),
    };

    let txc = tx.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(2));
        txc.send(NodeCommand::InfraSyncNow).unwrap()
    });

    std::thread::spawn(move || {
        use NodeCommand::*;
        loop {
            match rx.recv().unwrap() {
                Init { id, neighbours } => {
                    node.id = id;
                    node.neighbours = neighbours;
                }
                GetNodeId { sender } => sender
                    .send(node.get_node_id())
                    .expect("send node id through channel"),
                NextMsgId { sender } => {
                    let next_msg_id = node.next_msg_id();
                    sender.send(next_msg_id).unwrap();
                }
                LogAppend {
                    key,
                    msg,
                    sender_offset,
                } => {
                    let next_offset = find_next_offset_blocking(
                        &mut node,
                        &resp_router,
                        PREFIX_KEY_WRITE.to_string() + &key,
                    );

                    eprintln!("appending logs to offset {next_offset}");

                    if let Some(log) = node.logs.get_mut(&key) {
                        log.insert(next_offset, msg);
                    } else {
                        let mut v = Vec::new();
                        v.insert(next_offset, msg);
                        node.logs.insert(key.clone(), v);
                    }
                    sender_offset
                        .send(next_offset)
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
                ListCommittedOffsets {
                    client_id,
                    keys,
                    sender_offsets,
                } => {
                    let client_offsets = node.client_offsets(client_id, keys);
                    sender_offsets
                        .send(client_offsets)
                        .expect("client offsets sent");
                }
                InfraGetDiffs {
                    last_log_offset,
                    last_client_offsets,
                    sender_diff,
                } => {
                    let log_diffs = node.log_diffs(last_log_offset);
                    let client_offset_diffs = node.client_offset_diffs(last_client_offsets);
                    sender_diff.send((log_diffs, client_offset_diffs)).unwrap();
                }
                InfraSyncNow => {
                    eprintln!("init syncing with other nodes now!");
                    node.request_updates();
                }
                InfraApplyDiffs {
                    diff_logs,
                    diff_client_offsets,
                } => {
                    node.apply_diff_logs(diff_logs);
                    node.apply_diff_client_offsets(diff_client_offsets);
                }
            }
        }
    });
    tx
}

struct ResponseRouter {
    resp: HashMap<usize, mpsc::SyncSender<message::Message>>,
}

impl ResponseRouter {
    fn new() -> Self {
        Self {
            resp: HashMap::new(),
        }
    }

    fn register(&mut self, reply_to: usize, sender: mpsc::SyncSender<message::Message>) {
        self.resp.insert(reply_to, sender);
    }

    fn route(&mut self, reply_to: usize, msg: message::Message) -> Result<(), String> {
        if let Some(sender) = self.resp.remove(&reply_to) {
            sender
                .send(msg)
                .map_err(|e| format!("routing response err: {}", e))
        } else {
            Err("response not registered for given id".to_string())
        }
    }
}

fn send_wait(
    msg: message::Message,
    resp_router: &Arc<Mutex<ResponseRouter>>,
) -> Result<message::Message, String> {
    let (sender, receiver) = mpsc::sync_channel(1);
    let msg_id = msg.body.msg_id.unwrap();
    resp_router.lock().unwrap().register(msg_id, sender);
    send_msg(msg);
    receiver.recv().map_err(|e| format!("send wait err: {}", e))
}

const LIN_KV: &str = "lin-kv";

fn find_next_offset_blocking(
    node: &mut Node,
    resp_router: &Arc<Mutex<ResponseRouter>>,
    key: String,
) -> usize {
    let cur_offset = match get_offset_for_key(node, &key, resp_router) {
        Offset::Ok { value } => value,
        Offset::JustInitialized => {
            // compare-and-swap is not required
            return 0;
        }
    };
    eprintln!("got cur offset: {cur_offset}");

    let next_offset_for_key = cur_offset + 1;
    // compare-and-swap to commit next offset value
    let cas_resp = send_wait(
        message::Message::new_request(
            node.get_node_id(),
            LIN_KV.to_string(),
            node.next_msg_id(),
            message::Payload::Cas {
                key: key.clone(),
                from: cur_offset,
                to: next_offset_for_key.clone(),
            },
        ),
        resp_router,
    )
    .unwrap();

    match cas_resp.body.payload {
        message::Payload::Error { code, text } => {
            if code == 22 {
                eprint!("cas from value didn't match, repeat: {text}");
                return find_next_offset_blocking(node, resp_router, key);
            } else {
                panic!("unexpected error: {code} {text}");
            }
        }
        message::Payload::CasOk {} => {
            return next_offset_for_key;
        }
        _ => panic!("unsupported response type"),
    }
}

enum Offset {
    Ok { value: usize },
    JustInitialized,
}

fn get_offset_for_key(
    node: &mut Node,
    key: &String,
    resp_router: &Arc<Mutex<ResponseRouter>>,
) -> Offset {
    let read_resp = send_wait(
        message::Message::new_request(
            node.get_node_id(),
            LIN_KV.to_string(),
            node.next_msg_id(),
            message::Payload::Read { key: key.clone() },
        ),
        resp_router,
    )
    .unwrap();
    match read_resp.body.payload {
        message::Payload::Error { code, text } => {
            if code == 20 {
                eprintln!("key doesn't exist, init empty key");
                // key doesn't exist -> init new record
                let write_resp = send_wait(
                    message::Message::new_request(
                        node.get_node_id(),
                        LIN_KV.to_string(),
                        node.next_msg_id(),
                        message::Payload::Write {
                            key: key.clone(),
                            value: 0,
                        },
                    ),
                    resp_router,
                )
                .unwrap();

                match write_resp.body.payload {
                    message::Payload::WriteOk => {
                        return Offset::JustInitialized;
                    }
                    message::Payload::Error { code, text } => {
                        panic!("write error {code}: {text}");
                    }
                    _ => panic!("unsupported response type"),
                }
            } else {
                panic!("read error: {code} {text}")
            }
        }
        message::Payload::ReadOk { value } => Offset::Ok { value },
        _ => panic!("unsupported response type"),
    }
}

#[cfg(test)]
mod tests {
    use crate::message::LogDiff;

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
            neighbours: Vec::new(),
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
            neighbours: Vec::new(),
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

    #[test]
    fn node_log_diffs() {
        let node = Node {
            id: "id".to_string(),
            logs: HashMap::from([
                ("k1".to_string(), vec![1, 2, 3, 4]),
                ("k2".to_string(), vec![10, 20, 30, 40]),
                ("k3".to_string(), vec![100, 200, 300, 400]),
                ("k4".to_string(), vec![1000, 2000, 3000, 4000]),
            ]),
            msg_id_cnt: 0,
            client_offsets: HashMap::new(),
            neighbours: Vec::new(),
        };

        let mut expected = vec![
            message::LogDiff {
                key: "k1".to_string(),
                messages: vec![3, 4],
                starting_offset: 2,
            },
            message::LogDiff {
                key: "k2".to_string(),
                messages: vec![40],
                starting_offset: 3,
            },
            message::LogDiff {
                key: "k3".to_string(),
                messages: vec![200, 300, 400],
                starting_offset: 1,
            },
            message::LogDiff {
                key: "k4".to_string(),
                messages: vec![1000, 2000, 3000, 4000],
                starting_offset: 0,
            },
        ];
        expected.sort();
        assert_eq!(
            expected,
            node.log_diffs(HashMap::from([
                ("k1".to_string(), 1),
                ("k2".to_string(), 2),
                ("k3".to_string(), 0),
            ]))
        );
    }

    #[test]
    fn node_client_offset_diffs() {
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
            neighbours: Vec::new(),
        };

        assert_eq!(
            HashMap::from([
                (
                    "c1".to_string(),
                    HashMap::from([("k1".to_string(), 5), ("k3".to_string(), 15),])
                ),
                (
                    "c2".to_string(),
                    HashMap::from([
                        ("k1".to_string(), 50),
                        ("k2".to_string(), 100),
                        ("k3".to_string(), 150),
                    ])
                )
            ]),
            node.client_offset_diffs(HashMap::from([(
                "c1".to_string(),
                HashMap::from([
                    ("k1".to_string(), 3),
                    ("k2".to_string(), 10),
                    ("k3".to_string(), 10),
                ])
            )]))
        );
    }

    #[test]
    fn node_apply_diff_logs() {
        let mut node = Node {
            id: "id".to_string(),
            logs: HashMap::from([
                ("k1".to_string(), vec![1, 2, 3, 4]),
                ("k2".to_string(), vec![10, 20, 30, 40]),
                ("k3".to_string(), vec![100, 200, 300, 400]),
                ("k4".to_string(), vec![1000, 2000, 3000, 4000]),
            ]),
            msg_id_cnt: 0,
            client_offsets: HashMap::new(),
            neighbours: Vec::new(),
        };

        // green path scenarios
        let diff_logs = vec![
            LogDiff {
                key: "k1".to_string(),
                messages: vec![5, 6, 7, 8],
                starting_offset: 4,
            },
            LogDiff {
                key: "k2".to_string(),
                messages: vec![30, 40, 50],
                starting_offset: 2,
            },
            LogDiff {
                key: "k3".to_string(),
                messages: vec![111],
                starting_offset: 0,
            },
            LogDiff {
                key: "k5".to_string(),
                messages: vec![10_000, 20_000],
                starting_offset: 0,
            },
        ];
        node.apply_diff_logs(diff_logs);
        assert_eq!(
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            *node.logs.get(&"k1".to_string()).unwrap()
        );
        assert_eq!(
            vec![10, 20, 30, 40, 50],
            *node.logs.get(&"k2".to_string()).unwrap()
        );
        assert_eq!(
            vec![111, 200, 300, 400],
            *node.logs.get(&"k3".to_string()).unwrap()
        );
        assert_eq!(
            vec![10_000, 20_000],
            *node.logs.get(&"k5".to_string()).unwrap()
        );

        // panic: there is a gap in message indexes
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.apply_diff_logs(vec![LogDiff {
                key: "k4".to_string(),
                messages: vec![6000],
                starting_offset: 5,
            }])
        }));
        assert!(res.is_err());

        // panic: a new message key not starting from 0 index
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.apply_diff_logs(vec![LogDiff {
                key: "k6".to_string(),
                messages: vec![6000],
                starting_offset: 2,
            }])
        }));
        assert!(res.is_err());
    }

    #[test]
    fn node_apply_client_offset_diffs() {
        let mut node = Node {
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
            neighbours: Vec::new(),
        };

        node.apply_diff_client_offsets(HashMap::from([
            ("c1".to_string(), HashMap::from([("k1".to_string(), 6)])),
            ("c3".to_string(), HashMap::from([("k1".to_string(), 12)])),
            ("c2".to_string(), HashMap::from([("k4".to_string(), 1)])),
        ]));

        // insert new client
        assert_eq!(
            HashMap::from([("k1".to_string(), 12)]),
            *node.client_offsets.get(&"c3".to_string()).unwrap()
        );

        // offset update
        assert_eq!(
            6,
            *node
                .client_offsets
                .get(&"c1".to_string())
                .unwrap()
                .get(&"k1".to_string())
                .unwrap()
        );

        // existing client's new key
        assert_eq!(
            1,
            *node
                .client_offsets
                .get(&"c2".to_string())
                .unwrap()
                .get(&"k4".to_string())
                .unwrap()
        );
    }
}
