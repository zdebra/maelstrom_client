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

fn process_msg(msg: message::Message, node_tx: mpsc::Sender<NodeCommand>) {
    use message::Payload::*;
    match msg.body.payload.clone() {
        Init { node_id, node_ids } => {
            eprintln!("initializing node..");
            node_init(node_tx.clone(), node_id);
            send_reply(msg, node_tx.clone(), message::Payload::InitOk);
        }
        Topology { topology } => todo!(),
        // Error { code, text } => todo!(),
        Send { key, msg } => todo!(),
        // SendOk { offset } => todo!(),
        Poll { offsets } => todo!(),
        // PollOk { msgs } => todo!(),
        CommitOffsets { offsets } => todo!(),
        // CommitOffsetsOk => todo!(),
        ListCommitedOffsets { keys } => todo!(),
        // ListCommitedOffsetsOk { offsets } => todo!(),
        Echo { echo } => {
            eprintln!("hadnling echo..");
            send_reply(msg, node_tx.clone(), message::Payload::EchoOk { echo });
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

enum NodeCommand {
    Init { id: String },
    GetNodeId { sender: mpsc::Sender<String> },
    NextMsgId { sender: mpsc::Sender<usize> },
}

struct Node {
    id: String,
    logs: HashMap<String, Vec<usize>>,
    msg_id_cnt: usize,
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
            }
        }
    });
    tx
}
