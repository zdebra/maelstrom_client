use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, Write},
    sync::mpsc,
    thread,
};
mod broadcaster;
mod message;
mod node;
mod txn_batcher;

fn main() {
    eprintln!("started receiving messages");
    let mut last_msg_id = 0;
    let mut kv = KVStore::new();
    let mut txn_batcher = txn_batcher::TxnBatcher::new();
    let mut node = node::Node::new();
    let mut broadcaster = broadcaster::Broadcaster::new();
    let mut receiving = true;
    let mut broadcasted = HashSet::new();

    let (tx, rx) = mpsc::channel();
    // receivie messages
    thread::spawn(move || loop {
        let msg = receive_msg();
        tx.send(msg).unwrap();
    });
    loop {
        let is_node_receiving = node.is_receiving();
        let cur_epoch = node.cur_epoch();
        if !is_node_receiving && receiving {
            eprintln!("broadcast phase started");
            txn_batcher.stop_receiving();
            init_broadcast(&mut node, &mut txn_batcher);
            receiving = false;
        }
        if is_node_receiving && !receiving {
            eprintln!("receiving phase started");
            txn_batcher.start_receiving();
            receiving = true;
        }

        if let Ok(msg) = rx.try_recv() {
            eprintln!("received msg");
            process_msg(
                msg,
                &mut last_msg_id,
                &mut txn_batcher,
                &mut node,
                &mut broadcaster,
            )
        }

        if !is_node_receiving && broadcaster.has_all(cur_epoch) && !broadcasted.contains(&cur_epoch)
        {
            eprintln!("got all messages for epoch {}", cur_epoch);
            let msgs_from_other_nodes = broadcaster.get_all_general(cur_epoch);
            let msgs_from_this_node = txn_batcher.last_batch();
            let mut all_msgs = {
                let mut v = Vec::new();
                v.extend(msgs_from_other_nodes);
                v.extend(msgs_from_this_node.to_general_trx());
                v
            };

            all_msgs.sort_by_key(|msg| msg.seq.clone());

            // 4. execute
            for trx in all_msgs {
                let op_requests = parse_op_req(trx.txn);
                let op_results = kv.apply(op_requests);

                // respond to all messages local to this node
                if let Some(local_tx) = msgs_from_this_node
                    .iter()
                    .find(|msg| msg.general_txn.seq == trx.seq)
                {
                    let txn = results_to_txn(op_results);
                    let msg_id = last_msg_id + 1;
                    last_msg_id += 1;
                    send_msg(message::Message {
                        src: node.get_id(),
                        dest: local_tx.reply_to.clone(),
                        body: message::MessageBody {
                            msg_id: Some(msg_id),
                            in_reply_to: Some(local_tx.in_reply_to.clone()),
                            payload: message::Payload::TxnOk { txn },
                        },
                    });
                }
            }
            broadcasted.insert(cur_epoch);
        }
    }
}

fn init_broadcast(node: &mut node::Node, txn_batcher: &mut txn_batcher::TxnBatcher) {
    let all_nodes = node.get_all_nodes();
    eprintln!(
        "node {} start broadcast for {} nodes",
        node.get_id(),
        all_nodes.len()
    );
    let txns = txn_batcher.last_batch();
    for neighbour_i in 0..all_nodes.len() {
        let neighbour = all_nodes.get(neighbour_i).unwrap();
        let neighbour_str = neighbour.to_string();
        if *neighbour == node.get_id() {
            continue;
        }

        let next_msg_id = node.next_msg_id();

        send_msg(message::Message::new_request(
            node.get_id(),
            neighbour_str.clone(),
            next_msg_id,
            message::Payload::BroadcastTxn {
                txns: txns.to_seq_trx(),
            },
        ));
        eprintln!(
            "broadcasted {} transactions for node {} done",
            txns.len(),
            neighbour_str
        )
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

fn process_msg(
    req: message::Message,
    last_msg_id: &mut usize,
    txn_batcher: &mut txn_batcher::TxnBatcher,
    node: &mut node::Node,
    broadcaster: &mut broadcaster::Broadcaster,
) {
    *last_msg_id += 1;
    let next_msg_id = *last_msg_id;

    use message::Payload::*;
    match req.body.payload.clone() {
        Init { node_id, node_ids } => {
            node.init(node_id.clone(), node_ids.clone());
            broadcaster.init(node_ids, node_id);
            send_reply(next_msg_id, req, message::Payload::InitOk);
        }
        Txn { txn } => {
            let next_tx_seq = txn_batcher.next_seq();
            txn_batcher.push(LocalTxn {
                general_txn: GeneralTrx {
                    seq: TxnSeq {
                        seq_num: next_tx_seq,
                        node: node.get_id(),
                    },
                    txn,
                },
                reply_to: req.src,
                in_reply_to: req.body.msg_id.expect("message id is there"),
            });
        }
        BroadcastTxn { txns } => {
            let cur_epoch = node.cur_epoch();
            broadcaster.push(cur_epoch, node.get_id(), txns)
        }
        _ => {
            panic!("unexpected incoming message type")
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalTxn {
    general_txn: GeneralTrx,
    reply_to: String,   // who asked
    in_reply_to: usize, // original message id
}

#[derive(Debug, Eq, PartialEq, Ord, Clone)]
struct TxnSeq {
    seq_num: u64,
    node: String,
}

use core::cmp::Ordering;

impl PartialOrd for TxnSeq {
    fn partial_cmp(&self, other: &TxnSeq) -> Option<Ordering> {
        if self.node == other.node {
            self.seq_num.partial_cmp(&other.seq_num)
        } else {
            self.node.partial_cmp(&other.node)
        }
    }
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

trait ToGeneralTrx {
    fn to_general_trx(&self) -> Vec<GeneralTrx>;
}

impl ToGeneralTrx for Vec<LocalTxn> {
    fn to_general_trx(&self) -> Vec<GeneralTrx> {
        self.iter()
            .map(|local_txn| local_txn.general_txn.clone())
            .collect()
    }
}

trait ToSeqTrx {
    fn to_seq_trx(&self) -> Vec<message::SeqTxn>;
}

impl ToSeqTrx for Vec<LocalTxn> {
    fn to_seq_trx(&self) -> Vec<message::SeqTxn> {
        self.iter()
            .map(|local_txn| message::SeqTxn {
                seq: local_txn.general_txn.seq.seq_num,
                txn: local_txn.general_txn.txn.clone(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct GeneralTrx {
    seq: TxnSeq,
    txn: message::PlainTxn,
}
