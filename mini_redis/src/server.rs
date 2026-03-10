use crate::protocol::{parse_request, serialize_response, Request, RequestParseError, Response};
use std::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(addr: &str) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Listening on {addr}");

    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::info!("Accepted {peer}");
        tokio::spawn(async move {
            if let Err(err) = handle_client(socket).await {
                tracing::warn!("Connection {peer} closed with error: {err}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }

        let response = handle_line(line.trim_end_matches(['\r', '\n']));
        let payload = serialize_response(&response)?;
        writer.write_all(payload.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    Ok(())
}

fn handle_line(line: &str) -> Response {
    match parse_request(line) {
        Ok(Request::Ping) => Response::ok(),
        Err(RequestParseError::InvalidJson) => Response::error("invalid json"),
        Err(RequestParseError::UnknownCommand) => Response::error("unknown command"),
    }
}

#[cfg(test)]
mod tests {
    use super::handle_line;
    use crate::protocol::Response;

    #[test]
    fn ping_ok() {
        let resp = handle_line("{\"cmd\":\"PING\"}");
        assert_eq!(resp, Response::ok());
    }

    #[test]
    fn unknown_command() {
        let resp = handle_line("{\"cmd\":\"NOPE\"}");
        assert_eq!(resp.status, "error");
        assert_eq!(resp.message.as_deref(), Some("unknown command"));
    }

    #[test]
    fn invalid_json() {
        let resp = handle_line("not-json");
        assert_eq!(resp.status, "error");
        assert_eq!(resp.message.as_deref(), Some("invalid json"));
    }
}
