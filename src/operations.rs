use bytes::Bytes;
use std::{collections::VecDeque, time::Duration};

use crate::{data_store::DataStore, resp::RedisValueRef};

pub fn execute(operation: Vec<RedisValueRef>, store: DataStore) -> RedisValueRef {
    let command = match &operation[0] {
        RedisValueRef::String(s) => s.to_ascii_uppercase(),
        _ => return RedisValueRef::Error(Bytes::from("ERR invalid command")),
    };

    match command.as_slice() {
        b"PING" => ping(),
        b"ECHO" => echo(&operation),
        b"SET" => set(&operation, store),
        b"GET" => get(&operation, store),
        b"RPUSH" => rpush(&operation, store),
        b"LRANGE" => lrange(&operation, store),
        _ => RedisValueRef::Error(Bytes::from("ERR unknown command")),
    }
}

fn ping() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from("PONG"))
}

fn echo(value: &[RedisValueRef]) -> RedisValueRef {
    value
        .get(1)
        .cloned()
        .unwrap_or(RedisValueRef::NullBulkString)
}

fn set(arr: &[RedisValueRef], store: DataStore) -> RedisValueRef {
    let expiry = arr.len() == 5;
    if arr.len() != 3 && !expiry {
        return RedisValueRef::Error(Bytes::from("ERR invalid 'SET' command"));
    }
    let Some(key) = extract_string(&arr[1]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'SET' command: expected bulk string",
        ));
    };
    let Some(val) = extract_string(&arr[2]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'SET' command: expected bulk string",
        ));
    };
    let mut ex = None;
    if expiry {
        let Some(prec) = extract_string(&arr[3]) else {
            return RedisValueRef::Error(Bytes::from(
                "ERR invalid 'SET' command: expected bulk string",
            ));
        };
        let Some(dur) = extract_uint(&arr[4]) else {
            return RedisValueRef::Error(Bytes::from(
                "ERR invalid 'SET' command: expected bulk string",
            ));
        };
        ex = match prec.as_ref() {
            b"EX" => Some(Duration::from_secs(dur)),
            b"PX" => Some(Duration::from_millis(dur)),
            _ => {
                return RedisValueRef::Error(Bytes::from(
                    "ERR invalid 'SET' command: unknown additional argument",
                ));
            }
        }
    }
    let _ = store.set(key, val, ex);
    RedisValueRef::SimpleString(Bytes::from("OK"))
}

fn get(arr: &[RedisValueRef], store: DataStore) -> RedisValueRef {
    if arr.len() != 2 {
        return RedisValueRef::Error(Bytes::from("ERR invalid 'GET' command"));
    }
    let Some(key) = extract_string(&arr[1]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'SET' command: expected bulk string",
        ));
    };
    match store.get(&key) {
        Some(val) => RedisValueRef::String(val.clone()),
        None => RedisValueRef::NullBulkString,
    }
}

fn rpush(arr: &[RedisValueRef], store: DataStore) -> RedisValueRef {
    if arr.len() < 3 {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'RPUSH' command: incorrect number of arguments",
        ));
    }
    let Some(list_key) = extract_string(&arr[1]) else {
        return RedisValueRef::Error(Bytes::from("ERR invalid 'RPUSH' command: invalid list key"));
    };
    let mut values = VecDeque::new();
    for val in &arr[2..] {
        let Some(value) = extract_string(val) else {
            return RedisValueRef::Error(Bytes::from(
                "ERR invalid 'RPUSH' command: invalid list value",
            ));
        };
        values.push_back(value);
    }
    RedisValueRef::Int(store.rpush(list_key, values) as i64)
}

fn lrange(arr: &[RedisValueRef], store: DataStore) -> RedisValueRef {
    if arr.len() != 4 {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'LRANGE' command: incorrect number of arguments",
        ));
    }
    let Some(list_key) = extract_string(&arr[1]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'LRANGE' command: invalid list key",
        ));
    };
    let Some(start_index) = extract_int(&arr[2]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'LRANGE' command: invalid start index",
        ));
    };
    let Some(end_index) = extract_int(&arr[3]) else {
        return RedisValueRef::Error(Bytes::from(
            "ERR invalid 'LRANGE' command: invalid end index",
        ));
    };
    RedisValueRef::Array(
        store
            .lrange(list_key, start_index, end_index)
            .unwrap_or_default()
            .into_iter()
            .map(RedisValueRef::String)
            .collect(),
    )
}

fn extract_string(value: &RedisValueRef) -> Option<Bytes> {
    match value {
        RedisValueRef::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_uint(value: &RedisValueRef) -> Option<u64> {
    match value {
        RedisValueRef::String(s) => std::str::from_utf8(s).ok()?.parse().ok(),
        _ => None,
    }
}

fn extract_int(value: &RedisValueRef) -> Option<i64> {
    match value {
        RedisValueRef::String(s) => std::str::from_utf8(s).ok()?.parse().ok(),
        _ => None,
    }
}
