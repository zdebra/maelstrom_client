use crate::LocalTxn;

pub struct TxnBatcher {
    batch: Vec<LocalTxn>,
    receiving_stopped: bool,
    queue: Vec<LocalTxn>,
    seq: u64,
}

impl TxnBatcher {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            receiving_stopped: false,
            queue: Vec::new(),
            seq: 0,
        }
    }

    pub fn next_seq(&mut self) -> u64 {
        let seq = self.seq;
        self.seq += 1;
        seq
    }

    pub fn push(&mut self, txn: LocalTxn) {
        if self.receiving_stopped {
            self.queue.push(txn);
        } else {
            self.batch.push(txn);
        }
    }

    pub fn stop_receiving(&mut self) {
        self.receiving_stopped = true;
    }

    pub fn start_receiving(&mut self) {
        self.batch = self.queue.clone();
        self.queue.clear();
        self.receiving_stopped = false;
    }

    pub fn last_batch(&mut self) -> Vec<LocalTxn> {
        self.batch
            .iter()
            .map(|local_txn| local_txn.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_start_receiving() {
        let test_local_txn = LocalTxn {
            general_txn: crate::GeneralTrx {
                seq: crate::TxnSeq {
                    seq_num: 0,
                    node: "n1".to_string(),
                },
                txn: Vec::new(),
            },
            reply_to: "n1".to_string(),
            in_reply_to: 0,
        };
        let mut txn_batcher = TxnBatcher::new();
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.stop_receiving();
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        txn_batcher.push(test_local_txn.clone());
        assert_eq!(txn_batcher.last_batch().len(), 3);
        txn_batcher.start_receiving();
        txn_batcher.push(test_local_txn.clone());
        assert_eq!(txn_batcher.last_batch().len(), 6);
        txn_batcher.stop_receiving();
        assert_eq!(txn_batcher.last_batch().len(), 6);
        txn_batcher.start_receiving();
        assert!(txn_batcher.last_batch().is_empty());
    }
}
