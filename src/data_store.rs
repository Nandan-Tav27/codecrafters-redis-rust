use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;

#[derive(Clone)]
pub struct DataStore {
    store: Arc<Mutex<HashMap<Bytes, Value>>>,
}

pub enum Value {
    String(StringValue),
    List(VecDeque<Bytes>),
}

#[derive(Clone)]
pub struct StringValue {
    val: Bytes,
    ex: Option<Instant>,
}

impl DataStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let mut store = self.store.lock().unwrap();
        let value = store.get(key)?;
        match value {
            Value::String(value) => {
                if let Some(ex) = value.ex
                    && ex <= Instant::now()
                {
                    store.remove(key);
                    return None;
                }
                Some(value.val.clone())
            }
            _ => None,
        }
    }

    pub fn set(&self, key: Bytes, val: Bytes, ex: Option<Duration>) -> Option<Bytes> {
        let ex = ex.and_then(|d| Instant::now().checked_add(d));
        let prev = self
            .store
            .lock()
            .unwrap()
            .insert(key, Value::String(StringValue { val, ex }));
        match prev {
            Some(Value::String(value)) => Some(value.val),
            _ => None,
        }
    }

    pub fn rpush(&self, list_key: Bytes, values: VecDeque<Bytes>) -> usize {
        let mut store = self.store.lock().unwrap();
        let list = store
            .entry(list_key)
            .or_insert(Value::List(VecDeque::new()));
        match list {
            Value::List(l) => {
                l.extend(values);
                l.len()
            }
            _ => 0,
        }
    }

    pub fn lrange(&self, list_key: Bytes, start: usize, end: usize) -> Option<Vec<Bytes>> {
        if start > end {
            return None;
        }
        let store = self.store.lock().unwrap();
        let list = store.get(&list_key)?;
        match list {
            Value::List(l) => {
                if start >= l.len() {
                    return None;
                }
                let end = end.min(l.len() - 1);
                let values = l.range(start..=end).cloned().collect();
                Some(values)
            }
            _ => None,
        }
    }
}

