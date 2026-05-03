use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;

#[derive(Clone)]
pub struct DataStore {
    store: Arc<Mutex<HashMap<Bytes, Value>>>,
}

#[derive(Clone)]
pub struct Value {
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
        if let Some(ex) = value.ex
            && ex <= Instant::now()
        {
            store.remove(key);
            return None;
        }
        Some(value.val.clone())
    }

    pub fn set(&self, key: Bytes, val: Bytes, ex: Option<Duration>) -> Option<Bytes> {
        let ex = ex.and_then(|d| Instant::now().checked_add(d));
        self.store
            .lock()
            .unwrap()
            .insert(key, Value { val, ex })
            .map(|v| v.val)
    }
}

