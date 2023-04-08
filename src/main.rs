use anyhow::{Context, Ok, Result};
use std::io::{self, BufRead, Write};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
struct MessageBody {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    payload: Payload,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct Node {
    id: String,
    msg_counter: usize,
}

impl Node {
    fn step(&mut self, req: Message) -> Vec<Message> {
        let body = match req.body.payload {
            Payload::Init { node_id, .. } => {
                self.id = node_id;
                MessageBody {
                    msg_id: Some(self.msg_counter),
                    in_reply_to: req.body.msg_id,
                    payload: Payload::InitOk {},
                }
            }
            Payload::InitOk {} => panic!("unexpected message type InitOk received"),
            Payload::Echo { echo } => MessageBody {
                msg_id: Some(self.msg_counter),
                in_reply_to: req.body.msg_id,
                payload: Payload::EchoOk { echo },
            },
            Payload::EchoOk { .. } => {
                return vec![];
            }
        };

        self.msg_counter += 1;

        let resp = Message {
            src: req.dest,
            dest: req.src,
            body,
        };

        vec![resp]
    }
}

fn main() -> Result<()> {
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut node = Node {
        id: "uninitialized-node-id".to_string(),
        msg_counter: 0,
    };

    for line in stdin.lines() {
        let line_str = line.context("read line")?;
        let req: Message =
            serde_json::from_str(&line_str).context("serde deserialize msg from STDIN")?;
        dbg!("incomming message:", req.clone(), line_str);
        let responses = node.step(req);
        for resp in responses {
            let serialized_msg = serde_json::to_string(&resp).context("serialize Message")? + "\n";
            dbg!(resp, serialized_msg.clone());
            stdout.write_all(serialized_msg.as_bytes())?;
            stdout.flush()?;
        }
    }
    Ok(())
}
