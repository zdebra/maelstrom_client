use std::collections::HashMap;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

impl Message {
    pub fn new_request(src: String, dest: String, msg_id: usize, payload: Payload) -> Self {
        Self {
            src,
            dest,
            body: MessageBody {
                msg_id: Some(msg_id),
                in_reply_to: None,
                payload,
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct MessageBody {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Error {
        code: usize,
        text: String,
    },
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<usize>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    // KV-read
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
    },
    CasOk,
    Gossip {
        last_log_offset: HashMap<String, usize>, // key to offset
        last_client_offsets: HashMap<String, HashMap<String, usize>>, // client id to map of key to offset
    },
    GossipOk {
        diff_logs: Vec<LogDiff>,
        diff_client_offsets: HashMap<String, HashMap<String, usize>>, // client id to map of key to offset
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Ord, PartialOrd, Eq)]
pub struct LogDiff {
    pub key: String,
    pub messages: Vec<usize>,
    pub starting_offset: usize,
}
