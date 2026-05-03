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

    pub fn lpush(&self, list_key: Bytes, values: VecDeque<Bytes>) -> usize {
        let mut store = self.store.lock().unwrap();
        let list = store
            .entry(list_key)
            .or_insert(Value::List(VecDeque::new()));
        match list {
            Value::List(l) => {
                for value in values {
                    l.push_front(value);
                }
                l.len()
            }
            _ => 0,
        }
    }

    pub fn lpop_one(&self, list_key: Bytes) -> Option<Bytes> {
        let mut store = self.store.lock().unwrap();
        if let Some(Value::List(l)) = store.get_mut(&list_key) {
            l.pop_front()
        } else {
            None
        }
    }

    pub fn lpop_multiple(&self, list_key: Bytes, count: usize) -> Option<Vec<Bytes>> {
        let mut store = self.store.lock().unwrap();
        if let Some(Value::List(l)) = store.get_mut(&list_key)
            && !l.is_empty()
        {
            let count = count.min(l.len());
            Some(l.drain(..count).collect())
        } else {
            None
        }
    }

    pub fn lrange(&self, list_key: Bytes, start: i64, end: i64) -> Option<Vec<Bytes>> {
        let store = self.store.lock().unwrap();
        let list = store.get(&list_key)?;
        match list {
            Value::List(l) => {
                let new_start = normalize_index(start, l.len());
                let new_end = normalize_index(end, l.len());
                if new_start >= l.len() || new_start > new_end {
                    return None;
                }
                let new_end = new_end.min(l.len() - 1);
                let values = l.range(new_start..=new_end).cloned().collect();
                Some(values)
            }
            _ => None,
        }
    }

    pub fn llen(&self, list_key: Bytes) -> usize {
        let store = self.store.lock().unwrap();
        if let Some(Value::List(l)) = store.get(&list_key) {
            l.len()
        } else {
            0
        }
    }
}

fn normalize_index(idx: i64, len: usize) -> usize {
    if idx < 0 {
        if idx.unsigned_abs() as usize > len {
            0
        } else {
            (idx + len as i64) as usize
        }
    } else {
        idx as usize
    }
}
