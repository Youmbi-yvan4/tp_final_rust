use crate::error::AppError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

pub type Store = Arc<Mutex<HashMap<String, Entry>>>;

pub fn new_store() -> Store {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(store: &Store, key: String, value: String) {
    let mut guard = store.lock().expect("store poisoned");
    guard.insert(
        key,
        Entry {
            value,
            expires_at: None,
        },
    );
}

pub fn get(store: &Store, key: &str) -> Option<String> {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    guard.get(key).map(|e| e.value.clone())
}

pub fn del(store: &Store, key: &str) -> u64 {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    guard.remove(key).map(|_| 1).unwrap_or(0)
}

pub fn keys(store: &Store) -> Vec<String> {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    guard.keys().cloned().collect()
}

pub fn incr(store: &Store, key: &str, delta: i64) -> Result<i64, AppError> {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);

    let (current_str, expires_at) = match guard.remove(key) {
        Some(entry) => (entry.value, entry.expires_at),
        None => ("0".to_string(), None),
    };

    let current_val: i64 = current_str.parse().map_err(|_| AppError::NotInteger)?;
    let new_val = current_val.saturating_add(delta);
    guard.insert(
        key.to_string(),
        Entry {
            value: new_val.to_string(),
            expires_at,
        },
    );
    Ok(new_val)
}

pub fn expire(store: &Store, key: &str, seconds: u64) {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    if let Some(entry) = guard.get_mut(key) {
        entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
    }
}

pub fn ttl(store: &Store, key: &str) -> i64 {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    match guard.get(key) {
        None => -2,
        Some(entry) => match entry.expires_at {
            None => -1,
            Some(at) => {
                let now = Instant::now();
                if at <= now {
                    -2
                } else {
                    (at - now).as_secs() as i64
                }
            }
        },
    }
}

pub fn purge_expired(store: &Store) {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
}

pub fn snapshot(store: &Store) -> HashMap<String, String> {
    let mut guard = store.lock().expect("store poisoned");
    purge_expired_locked(&mut guard);
    guard
        .iter()
        .map(|(k, v)| (k.clone(), v.value.clone()))
        .collect()
}

fn purge_expired_locked(guard: &mut HashMap<String, Entry>) {
    let now = Instant::now();
    guard.retain(|_, entry| match entry.expires_at {
        Some(at) => at > now,
        None => true,
    });
}
