use std::collections::HashMap;

use crate::{message, GeneralTrx, TxnSeq};

pub struct Broadcaster {
    broadcast_nodes: HashMap<usize, HashMap<String, Vec<message::SeqTxn>>>, // broadcast epoch -> node id -> txns
    all_node_ids: Vec<String>,
}

impl Broadcaster {
    pub fn new() -> Self {
        Self {
            broadcast_nodes: HashMap::new(),
            all_node_ids: Vec::new(),
        }
    }

    pub fn init(&mut self, node_ids: Vec<String>) {
        self.all_node_ids = node_ids;
    }

    pub fn push(&mut self, epoch: usize, node_id: String, txns: Vec<message::SeqTxn>) {
        self.broadcast_nodes
            .entry(epoch)
            .or_insert_with(HashMap::new)
            .insert(node_id, txns);
    }

    pub fn has_all(&self, epoch: usize) -> bool {
        self.broadcast_nodes.get(&epoch).unwrap().len() == self.all_node_ids.len()
    }

    pub fn get_all_general(&self, epoch: usize) -> Vec<GeneralTrx> {
        self.broadcast_nodes
            .get(&epoch)
            .unwrap()
            .iter()
            .map(|(node_id, txns)| {
                txns.iter()
                    .map(|seq_txn| GeneralTrx {
                        seq: TxnSeq {
                            seq_num: seq_txn.seq,
                            node: node_id.clone(),
                        },
                        txn: seq_txn.txn.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message;
    #[test]
    fn push_and_get() {
        let mut broadcaster = Broadcaster::new();
        broadcaster.init(vec!["n1".to_string(), "n2".to_string()]);
        broadcaster.push(
            0,
            "n1".to_string(),
            vec![message::SeqTxn {
                seq: 2,
                txn: vec![vec![
                    Some(message::OperationValue::String("w".to_string())),
                    Some(message::OperationValue::Integer(3)),
                    Some(message::OperationValue::Integer(2)),
                ]],
            }],
        );

        let general_txn = broadcaster.get_all_general(0);
        assert_eq!(general_txn.len(), 1);
        assert_eq!(general_txn[0].seq.seq_num, 2);
        assert_eq!(general_txn[0].seq.node, "n1");
    }
}
