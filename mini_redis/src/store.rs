use std::collections::HashMap;
use std::sync::{Arc, Mutex};


pub type Store = Arc<Mutex<HashMap<String, String>>>;

pub fn new_store() -> Store {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(store: &Store, key: String, value: String) {
    let mut guard = store.lock().expect("store poisoned");
    guard.insert(key, value);
}

pub fn get(store: &Store, key: &str) -> Option<String> {
    let guard = store.lock().expect("store poisoned");
    guard.get(key).cloned()
}

pub fn del(store: &Store, key: &str) -> u64 {
    let mut guard = store.lock().expect("store poisoned");
    guard.remove(key).map(|_| 1).unwrap_or(0)

}
