use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
};

mod message;

fn main() {
    eprintln!("started receiving messages");
    let mut last_msg_id = 0;
    let mut kv = KVStore::new();
    loop {
        let msg = receive_msg();
        process_msg(msg, &mut last_msg_id, &mut kv);
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

fn send_reply(msg_id: usize, reply_to: message::Message, payload: message::Payload) {
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

fn process_msg(req: message::Message, last_msg_id: &mut usize, kv: &mut KVStore) {
    *last_msg_id += 1;
    let next_msg_id = *last_msg_id;

    let mut uncommitted_txns = Vec::new();
    let mut cur_node: Option<usize> = None;
    let mut next_tx_seq = 0;
    let mut all_nodes = Vec::new();

    use message::Payload::*;
    match req.body.payload.clone() {
        Init { node_id, node_ids } => {
            eprintln!("initializing node..");
            cur_node = Some(node_id);
            all_nodes = node_ids;
            send_reply(next_msg_id, req, message::Payload::InitOk);
        }
        Txn { txn } => {
            uncommitted_txns.push(LocalTxn {
                seq: next_tx_seq.clone(),
                origin: cur_node.expect("node was initiated"),
                txn,
                reply_to: req.src,
                in_reply_to: req.body.msg_id.expect("message id is there"),
            });
            next_tx_seq += 1;
            // let operations = parse_op_req(txn);
            // let results = kv.apply(operations);
            // send_reply(
            //     next_msg_id,
            //     req,
            //     message::Payload::TxnOk {
            //         txn: results_to_txn(results),
            //     },
            // )
        }
        BroadcastTxn { txns } => {
            // todo:    1. gather txns from all other nodes
            //          2. sort all txns
            //          3. exec all txns and reply to those that were asked this node
        }
        _ => {
            panic!("unexpected incoming message type")
        }
    }
}

struct LocalTxn {
    seq: usize,
    origin: usize,
    txn: message::PlainTxn,
    reply_to: String,   // who asked
    in_reply_to: usize, // original message id
}

enum OperationRequest {
    Read { key: usize },
    Write { key: usize, value: usize },
}

fn parse_op_req(inp: Vec<Vec<Option<message::OperationValue>>>) -> Vec<OperationRequest> {
    inp.into_iter()
        .map(|op| {
            assert_eq!(op.len(), 3);
            let op_type = match op[0].as_ref().expect("first item cannot be null") {
                message::OperationValue::String(s) => s,
                _ => panic!("unexpected type for op type"),
            };
            let key = match op[1].as_ref().expect("second item cannot be null") {
                message::OperationValue::Integer(i) => i,
                _ => panic!("unexpected type for key type"),
            };
            let value = &op[2];

            match op_type.as_str() {
                "w" => {
                    let value = match value.as_ref().expect("value cannot be null for write op") {
                        message::OperationValue::Integer(i) => i,
                        _ => panic!("unexpected type for value"),
                    };
                    OperationRequest::Write {
                        key: *key,
                        value: *value,
                    }
                }
                "r" => OperationRequest::Read { key: *key },
                _ => panic!("unsupported operation type"),
            }
        })
        .collect()
}

enum OperationResult {
    Read { key: usize, value: Option<usize> },
    Write { key: usize, value: usize },
}

fn results_to_txn(results: Vec<OperationResult>) -> Vec<Vec<Option<message::OperationValue>>> {
    use message::OperationValue;
    results
        .into_iter()
        .map(|op_res| match op_res {
            OperationResult::Read { key, value } => {
                let value = if let Some(v) = value {
                    Some(OperationValue::Integer(v))
                } else {
                    None
                };
                vec![
                    Some(OperationValue::String("r".to_string())),
                    Some(OperationValue::Integer(key)),
                    value,
                ]
            }
            OperationResult::Write { key, value } => vec![
                Some(OperationValue::String("w".to_string())),
                Some(OperationValue::Integer(key)),
                Some(OperationValue::Integer(value)),
            ],
        })
        .collect()
}

struct KVStore {
    kv: HashMap<usize, usize>,
}

impl KVStore {
    fn new() -> Self {
        Self { kv: HashMap::new() }
    }
    fn apply(&mut self, op_requests: Vec<OperationRequest>) -> Vec<OperationResult> {
        op_requests
            .into_iter()
            .map(|op_req| match op_req {
                OperationRequest::Read { key } => OperationResult::Read {
                    key,
                    value: self.kv.get(&key).copied(),
                },
                OperationRequest::Write { key, value } => {
                    self.kv.entry(key).and_modify(|v| *v = value);
                    OperationResult::Write { key, value }
                }
            })
            .collect()
    }
}
