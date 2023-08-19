use std::{time::Duration, time::SystemTime};

pub struct Node {
    id: Option<String>,
    all_nodes: Vec<String>,
    receiving_dur: Duration,
    executing_dur: Duration,
    start_at: SystemTime,
    msg_id: usize,
}

impl Node {
    pub fn new() -> Self {
        Self {
            id: None,
            all_nodes: Vec::new(),
            receiving_dur: Duration::from_millis(500),
            executing_dur: Duration::from_millis(500),
            start_at: SystemTime::now(),
            msg_id: 0,
        }
    }

    pub fn get_id(&self) -> String {
        self.id.clone().unwrap()
    }

    pub fn get_all_nodes(&self) -> Vec<String> {
        self.all_nodes.clone()
    }

    pub fn init(&mut self, id: String, all_nodes: Vec<String>) {
        eprintln!(
            "initializing node with id {} and all_nodes {:?}",
            id, all_nodes
        );
        self.id = Some(id);
        self.all_nodes = all_nodes;
    }

    pub fn is_receiving(&self) -> bool {
        let total_dur = self.receiving_dur + self.executing_dur;
        let since_start = self.start_at.elapsed().unwrap();
        let cur_epoch_dur = since_start.as_nanos() % total_dur.as_nanos();
        cur_epoch_dur < self.receiving_dur.as_nanos()
    }

    pub fn next_msg_id(&mut self) -> usize {
        let msg_id = self.msg_id;
        self.msg_id += 1;
        msg_id
    }

    pub fn cur_epoch(&self) -> usize {
        let epoch_dur = self.receiving_dur + self.executing_dur;
        let since_start = self.start_at.elapsed().unwrap();
        let a = since_start.as_nanos();
        let b = epoch_dur.as_nanos();
        ((a + (b - 1)) / b) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn node_is_receiving() {
        let mut node = Node::new();
        node.receiving_dur = Duration::from_millis(5);
        node.executing_dur = Duration::from_millis(5);
        node.start_at = SystemTime::now();
        assert!(node.is_receiving());
        std::thread::sleep(Duration::from_millis(5));
        assert!(!node.is_receiving());
        std::thread::sleep(Duration::from_millis(5));
        assert!(node.is_receiving());
    }

    #[test]
    fn cur_epoch() {
        let mut node = Node::new();
        node.receiving_dur = Duration::from_millis(5);
        node.executing_dur = Duration::from_millis(5);
        node.start_at = SystemTime::now();
        assert_eq!(node.cur_epoch(), 0);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(node.cur_epoch(), 1);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(node.cur_epoch(), 2);
    }
}
