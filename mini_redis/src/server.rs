use crate::protocol::{parse_request, serialize_response, Request, RequestParseError, Response};
use crate::store::{del, expire, get, incr, keys, new_store, purge_expired, set, snapshot, ttl, Store};
use serde_json::Value;
use std::error::Error;
use std::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};

pub async fn run(addr: &str) -> Result<(), Box<dyn Error>> {
    let store = new_store();
    tokio::spawn(clean_expired(store.clone()));
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Listening on {addr}");

    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::info!("Accepted {peer}");
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(socket, store).await {
                tracing::warn!("Connection {peer} closed with error: {err}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream, store: Store) -> Result<(), Box<dyn Error>> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }

        let response = handle_line(line.trim_end_matches(['\r', '\n']), &store);
        let payload = serialize_response(&response)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    Ok(())
}

fn handle_line(line: &str, store: &Store) -> Response {
    match parse_request(line) {
        Ok(Request::Ping) => Response::ok(),
        Ok(Request::Set { key, value }) => {
            set(store, key, value);
            Response::ok()
        }
        Ok(Request::Get { key }) => {
            let value = get(store, &key).map(Value::String).unwrap_or(Value::Null);
            Response {
                value: Some(value),
                ..Response::ok()
            }
        }
        Ok(Request::Del { key }) => Response {
            count: Some(del(store, &key)),
            ..Response::ok()
        },
        Ok(Request::Keys) => Response {
            keys: Some(keys(store)),
            ..Response::ok()
        },
        Ok(Request::Expire { key, seconds }) => {
            expire(store, &key, seconds);
            Response::ok()
        }
        Ok(Request::Ttl { key }) => Response {
            ttl: Some(ttl(store, &key)),
            ..Response::ok()
        },
        Ok(Request::Incr { key }) => match incr(store, &key, 1) {
            Ok(val) => Response {
                value: Some(Value::Number(val.into())),
                ..Response::ok()
            },
            Err(msg) => Response::error(msg),
        },
        Ok(Request::Decr { key }) => match incr(store, &key, -1) {
            Ok(val) => Response {
                value: Some(Value::Number(val.into())),
                ..Response::ok()
            },
            Err(msg) => Response::error(msg),
        },
        Ok(Request::Save) => match save_dump(store) {
            Ok(_) => Response::ok(),
            Err(msg) => Response::error(msg),
        },
        Err(RequestParseError::InvalidJson) => Response::error("invalid json"),
        Err(RequestParseError::UnknownCommand) => Response::error("unknown command"),
    }
}

fn save_dump(store: &Store) -> Result<(), String> {
    let data = snapshot(store);
    let serialized = serde_json::to_vec_pretty(&data).map_err(|e| e.to_string())?;
    fs::write("dump.json", serialized).map_err(|e| e.to_string())
}

async fn clean_expired(store: Store) {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        purge_expired(&store);
    }
}

#[cfg(test)]
mod tests {
    use super::handle_line;
    use crate::protocol::Response;
    use crate::store::new_store;
    use serde_json::Value;

    #[test]
    fn ping_ok() {
        let store = new_store();
        let resp = handle_line("{\"cmd\":\"PING\"}", &store);
        assert_eq!(resp, Response::ok());
    }

    #[test]
    fn unknown_command() {
        let store = new_store();
        let resp = handle_line("{\"cmd\":\"NOPE\"}", &store);
        assert_eq!(resp.status, "error");
        assert_eq!(resp.message.as_deref(), Some("unknown command"));
    }

    #[test]
    fn invalid_json() {
        let store = new_store();
        let resp = handle_line("not-json", &store);
        assert_eq!(resp.status, "error");
        assert_eq!(resp.message.as_deref(), Some("invalid json"));
    }

    #[test]
    fn set_get_roundtrip() {
        let store = new_store();
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"a\",\"value\":\"b\"}", &store);
        let resp = handle_line("{\"cmd\":\"GET\",\"key\":\"a\"}", &store);
        assert_eq!(resp.status, "ok");
        assert_eq!(resp.value, Some(Value::String("b".into())));
    }

    #[test]
    fn del_removes_key() {
        let store = new_store();
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"a\",\"value\":\"b\"}", &store);
        let resp = handle_line("{\"cmd\":\"DEL\",\"key\":\"a\"}", &store);
        assert_eq!(resp.count, Some(1));
        let resp = handle_line("{\"cmd\":\"GET\",\"key\":\"a\"}", &store);
        assert_eq!(resp.value, Some(Value::Null));
    }

    #[test]
    fn keys_lists_all() {
        let store = new_store();
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"a\",\"value\":\"1\"}", &store);
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"b\",\"value\":\"2\"}", &store);
        let resp = handle_line("{\"cmd\":\"KEYS\"}", &store);
        let mut returned = resp.keys.clone().unwrap_or_default();
        returned.sort();
        assert_eq!(returned, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn expire_and_ttl() {
        use std::time::{Duration, Instant};

        let store = new_store();
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"a\",\"value\":\"1\"}", &store);
        let _ = handle_line("{\"cmd\":\"EXPIRE\",\"key\":\"a\",\"seconds\":1}", &store);
        let ttl_resp = handle_line("{\"cmd\":\"TTL\",\"key\":\"a\"}", &store);
        assert!(ttl_resp.ttl.unwrap() >= 0);

        // Simulate passage of time deterministically by forcing expiration instant to past, then purging.
        {
            let mut guard = store.lock().unwrap();
            if let Some(entry) = guard.get_mut("a") {
                entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            }
        }
        crate::store::purge_expired(&store);
        let ttl_resp = handle_line("{\"cmd\":\"TTL\",\"key\":\"a\"}", &store);
        assert_eq!(ttl_resp.ttl, Some(-2));
    }

    #[test]
    fn incr_and_decr() {
        let store = new_store();
        let resp = handle_line("{\"cmd\":\"INCR\",\"key\":\"c\"}", &store);
        assert_eq!(resp.value, Some(Value::Number(1.into())));
        let resp = handle_line("{\"cmd\":\"DECR\",\"key\":\"c\"}", &store);
        assert_eq!(resp.value, Some(Value::Number(0.into())));
    }

    #[test]
    fn incr_invalid_number() {
        let store = new_store();
        let _ = handle_line("{\"cmd\":\"SET\",\"key\":\"n\",\"value\":\"abc\"}", &store);
        let resp = handle_line("{\"cmd\":\"INCR\",\"key\":\"n\"}", &store);
        assert_eq!(resp.status, "error");
        assert_eq!(resp.message.as_deref(), Some("not an integer"));
    }

    #[test]
    fn save_ok() {
        let store = new_store();
        let resp = handle_line("{\"cmd\":\"SAVE\"}", &store);
        assert_eq!(resp.status, "ok");
    }
}
