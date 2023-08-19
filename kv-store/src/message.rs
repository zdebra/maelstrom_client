use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    Txn {
        txn: PlainTxn,
    },
    TxnOk {
        txn: PlainTxn,
    },

    BroadcastTxn {
        txns: Vec<SeqTxn>,
    },
}

pub type PlainTxn = Vec<Vec<Option<OperationValue>>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum OperationValue {
    Integer(usize),
    String(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SeqTxn {
    pub seq: u128,
    pub txn: PlainTxn,
}
