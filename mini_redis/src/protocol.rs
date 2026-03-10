use serde::Serialize;
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum Request {
    Ping,
    Get { key: String },
    Set { key: String, value: String },
    Del { key: String },
    Keys,
    Expire { key: String, seconds: u64 },
    Ttl { key: String },
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
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            status: "ok",
            value: None,
            count: None,
            keys: None,
            ttl: None,
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error",
            value: None,
            count: None,
            keys: None,
            ttl: None,
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
        "GET" => {
            let key = value
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            Ok(Request::Get { key })
        }
        "SET" => {
            let key = value
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            let value = value
                .get("value")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            Ok(Request::Set { key, value })
        }
        "DEL" => {
            let key = value
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            Ok(Request::Del { key })
        }
        "KEYS" => Ok(Request::Keys),
        "EXPIRE" => {
            let key = value
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            let seconds = value
                .get("seconds")
                .and_then(|v| v.as_u64())
                .ok_or(RequestParseError::InvalidJson)?;
            Ok(Request::Expire { key, seconds })
        }
        "TTL" => {
            let key = value
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or(RequestParseError::InvalidJson)?
                .to_string();
            Ok(Request::Ttl { key })
        }
        _ => Err(RequestParseError::UnknownCommand),
    }
}

pub fn serialize_response(resp: &Response) -> Result<String, serde_json::Error> {
    serde_json::to_string(resp)
}
