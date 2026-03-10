use crate::protocol::{parse_request, serialize_response, Request, RequestParseError, Response};
use crate::store::{del, get, new_store, set, Store};
use serde_json::Value;
use std::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(addr: &str) -> Result<(), Box<dyn Error>> {
    let store = new_store();
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
        Err(RequestParseError::InvalidJson) => Response::error("invalid json"),
        Err(RequestParseError::UnknownCommand) => Response::error("unknown command"),
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
}
