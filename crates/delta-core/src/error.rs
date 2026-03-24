use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    TableNotFound,
    VersionNotFound,
    PermissionDenied,
    CorruptLog,
    IoError,
    QueryError,
    Internal,
}

impl ErrorCode {
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorCode::IoError | ErrorCode::Internal)
    }
}

#[derive(Error, Debug)]
pub enum DeltaViewerError {
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Delta error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] deltalake::datafusion::error::DataFusionError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl DeltaViewerError {
    pub fn error_code(&self) -> ErrorCode {
        match self {
            DeltaViewerError::Parquet(_)
            | DeltaViewerError::Arrow(_)
            | DeltaViewerError::DataFusion(_) => ErrorCode::QueryError,
            DeltaViewerError::Delta(e) => {
                let msg = e.to_string().to_lowercase();
                if msg.contains("not found") {
                    ErrorCode::TableNotFound
                } else if msg.contains("version") {
                    ErrorCode::VersionNotFound
                } else {
                    ErrorCode::Internal
                }
            }
            DeltaViewerError::Io(e) => {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    ErrorCode::PermissionDenied
                } else {
                    ErrorCode::IoError
                }
            }
            DeltaViewerError::Json(_) | DeltaViewerError::Other(_) => ErrorCode::Internal,
        }
    }
}

pub type Result<T> = std::result::Result<T, DeltaViewerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_io_permission() {
        let err = DeltaViewerError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "access denied",
        ));
        assert_eq!(err.error_code(), ErrorCode::PermissionDenied);
        assert!(!err.error_code().is_retryable());
    }

    #[test]
    fn test_error_code_io_other() {
        let err = DeltaViewerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(err.error_code(), ErrorCode::IoError);
        assert!(err.error_code().is_retryable());
    }

    #[test]
    fn test_error_code_json() {
        let json_err: std::result::Result<serde_json::Value, _> = serde_json::from_str("{bad");
        let err = DeltaViewerError::Json(json_err.unwrap_err());
        assert_eq!(err.error_code(), ErrorCode::Internal);
        assert!(err.error_code().is_retryable());
    }

    #[test]
    fn test_error_code_other() {
        let err = DeltaViewerError::Other("something".into());
        assert_eq!(err.error_code(), ErrorCode::Internal);
    }

    #[test]
    fn test_error_code_query() {
        let err = DeltaViewerError::Arrow(arrow::error::ArrowError::ComputeError(
            "bad compute".into(),
        ));
        assert_eq!(err.error_code(), ErrorCode::QueryError);
        assert!(!err.error_code().is_retryable());
    }
}
