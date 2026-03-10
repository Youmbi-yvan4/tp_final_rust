use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("not an integer")]
    NotInteger,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}
