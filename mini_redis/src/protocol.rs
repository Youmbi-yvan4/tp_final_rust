use serde::Serialize;
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum Request {
    Ping,
}

#[derive(Debug, PartialEq)]
pub enum RequestParseError {
    InvalidJson,
    UnknownCommand,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Response {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            status: "ok",
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error",
            message: Some(message.into()),
        }
    }
}

pub fn parse_request(line: &str) -> Result<Request, RequestParseError> {
    let value: Value = serde_json::from_str(line).map_err(|_| RequestParseError::InvalidJson)?;
    let cmd = value
        .get("cmd")
        .and_then(|v| v.as_str())
        .ok_or(RequestParseError::InvalidJson)?;

    match cmd {
        "PING" => Ok(Request::Ping),
        _ => Err(RequestParseError::UnknownCommand),
    }
}

pub fn serialize_response(resp: &Response) -> Result<String, serde_json::Error> {
    serde_json::to_string(resp)
}
