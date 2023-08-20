use crate::LocalTxn;

pub struct TxnBatcher {
    batch: Vec<LocalTxn>,
    receiving_stopped: bool,
    queue: Vec<LocalTxn>,
    seq: u128,
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

    pub fn next_seq(&mut self) -> u128 {
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
        self.batch.clear();
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
