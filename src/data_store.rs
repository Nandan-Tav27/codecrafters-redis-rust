use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use tokio::{sync::oneshot, time::timeout};

#[derive(Clone)]
pub struct DataStore {
    value_store: Arc<Mutex<HashMap<Bytes, Value>>>,
    channel_store: Arc<Mutex<HashMap<Bytes, VecDeque<oneshot::Sender<Bytes>>>>>,
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
            value_store: Arc::new(Mutex::new(HashMap::new())),
            channel_store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let mut store = self.value_store.lock().unwrap();
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
            .value_store
            .lock()
            .unwrap()
            .insert(key, Value::String(StringValue { val, ex }));
        match prev {
            Some(Value::String(value)) => Some(value.val),
            _ => None,
        }
    }

    pub fn rpush(&self, list_key: Bytes, values: VecDeque<Bytes>) -> usize {
        let mut store = self.value_store.lock().unwrap();
        let list = store
            .entry(list_key.clone())
            .or_insert(Value::List(VecDeque::new()));
        match list {
            Value::List(l) => {
                l.extend(values);
                let len = l.len();
                self.notify_waker(&list_key, l);
                len
            }
            _ => 0,
        }
    }

    pub fn lpush(&self, list_key: Bytes, values: VecDeque<Bytes>) -> usize {
        let mut store = self.value_store.lock().unwrap();
        let list = store
            .entry(list_key.clone())
            .or_insert(Value::List(VecDeque::new()));
        match list {
            Value::List(l) => {
                for value in values {
                    l.push_front(value);
                }
                let len = l.len();
                self.notify_waker(&list_key, l);
                len
            }
            _ => 0,
        }
    }

    pub fn lpop_one(&self, list_key: &Bytes) -> Option<Bytes> {
        let mut store = self.value_store.lock().unwrap();
        if let Some(Value::List(l)) = store.get_mut(list_key) {
            l.pop_front()
        } else {
            None
        }
    }

    pub fn lpop_multiple(&self, list_key: Bytes, count: usize) -> Option<Vec<Bytes>> {
        let mut store = self.value_store.lock().unwrap();
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
        let store = self.value_store.lock().unwrap();
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
        let store = self.value_store.lock().unwrap();
        if let Some(Value::List(l)) = store.get(&list_key) {
            l.len()
        } else {
            0
        }
    }

    pub fn type_op(&self, list_key: &Bytes) -> Option<Bytes> {
        let store = self.value_store.lock().unwrap();
        match store.get(list_key).unwrap() {
            Value::String(_) => Some(Bytes::from("string")),
            Value::List(_) => Some(Bytes::from("list")),
        }
    }

    // --- blocking ops ---

    pub async fn blpop(&self, list_key: Bytes, dur: Duration) -> Option<(Bytes, Bytes)> {
        if let Some(value) = self.lpop_one(&list_key) {
            Some((list_key, value))
        } else {
            let rx = {
                let (tx, rx) = oneshot::channel();
                let mut channel_store = self.channel_store.lock().unwrap();
                channel_store
                    .entry(list_key.clone())
                    .or_default()
                    .push_back(tx);
                rx
            };
            if dur == Duration::from_secs_f64(0 as f64) {
                let res = rx.await.unwrap();
                Some((list_key, res))
            } else {
                match timeout(dur, rx).await {
                    Ok(Ok(res)) => Some((list_key, res)),
                    _ => None,
                }
            }
        }
    }

    fn notify_waker(&self, list_key: &Bytes, list: &mut VecDeque<Bytes>) {
        let mut channel_store = self.channel_store.lock().unwrap();
        if let Some(waiters) = channel_store.get_mut(list_key)
            && let Some(tx) = waiters.pop_front()
            && let Some(val) = list.pop_front()
        {
            let _ = tx.send(val);
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
