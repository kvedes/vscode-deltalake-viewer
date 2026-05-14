use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Categorized error codes returned to the VS Code extension.
///
/// These codes let the frontend decide how to present errors to the user
/// (e.g., showing a "table not found" message vs. offering a retry button).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The specified Delta table path does not exist or is not a Delta table.
    TableNotFound,
    /// The requested table version does not exist.
    VersionNotFound,
    /// Filesystem permission denied when accessing the table.
    PermissionDenied,
    /// The Delta transaction log is corrupted or unreadable.
    CorruptLog,
    /// A general I/O error occurred (e.g., network or disk failure).
    IoError,
    /// An error occurred during query execution (Arrow, Parquet, or DataFusion).
    QueryError,
    /// An unexpected internal error.
    Internal,
}

impl ErrorCode {
    /// Returns `true` if the error is transient and the operation may succeed on retry.
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorCode::IoError)
    }
}

/// Unified error type for all delta-core operations.
///
/// Wraps errors from the Parquet, Arrow, Delta, DataFusion, JSON, and I/O layers
/// and maps them to an [`ErrorCode`] for structured error responses.
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
    /// Maps this error to a structured [`ErrorCode`] based on the underlying cause.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            DeltaViewerError::Parquet(_)
            | DeltaViewerError::Arrow(_)
            | DeltaViewerError::DataFusion(_) => ErrorCode::QueryError,
            DeltaViewerError::Delta(e) => match e {
                deltalake::DeltaTableError::NotATable(_)
                | deltalake::DeltaTableError::InvalidTableLocation(_) => ErrorCode::TableNotFound,
                deltalake::DeltaTableError::InvalidVersion(_)
                | deltalake::DeltaTableError::VersionAlreadyExists(_)
                | deltalake::DeltaTableError::VersionMismatch(_, _) => ErrorCode::VersionNotFound,
                _ => ErrorCode::Internal,
            },
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

/// Convenience alias for results using [`DeltaViewerError`].
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
        assert!(!err.error_code().is_retryable());
    }

    #[test]
    fn test_error_code_other() {
        let err = DeltaViewerError::Other("something".into());
        assert_eq!(err.error_code(), ErrorCode::Internal);
    }

    #[test]
    fn test_error_code_query() {
        let err =
            DeltaViewerError::Arrow(arrow::error::ArrowError::ComputeError("bad compute".into()));
        assert_eq!(err.error_code(), ErrorCode::QueryError);
        assert!(!err.error_code().is_retryable());
    }

    // -----------------------------------------------------------------------
    // Failing tests added to cover bugs documented in /review.md.
    // Each test asserts the *desired* behavior; until the bug is fixed
    // these tests will fail.
    // -----------------------------------------------------------------------

    /// review.md §1.3 — Error code classification matches localized substrings
    /// of the underlying error message. A `DeltaTableError::Generic` whose text
    /// merely *mentions* "version" is misclassified as `VersionNotFound`.
    #[test]
    fn bug_1_3_delta_error_msg_with_version_is_not_version_not_found() {
        let err = DeltaViewerError::Delta(deltalake::DeltaTableError::Generic(
            "schema validation failed for version field in metadata".to_string(),
        ));
        // We expect this to fall through to Internal — it has nothing to do
        // with a missing table version. Today the substring match catches the
        // word "version" and miscategorizes the error.
        assert_eq!(
            err.error_code(),
            ErrorCode::Internal,
            "Generic delta error mentioning 'version' should not be classified as VersionNotFound",
        );
    }

    /// review.md §1.3 — Same problem for "not found" appearing in unrelated
    /// messages.
    #[test]
    fn bug_1_3_delta_error_msg_with_not_found_is_not_table_not_found() {
        let err = DeltaViewerError::Delta(deltalake::DeltaTableError::Generic(
            "partition column 'foo' was not found in schema".to_string(),
        ));
        assert_eq!(
            err.error_code(),
            ErrorCode::Internal,
            "Generic delta error mentioning 'not found' in an unrelated context should not be classified as TableNotFound",
        );
    }

    /// review.md §1.4 — `ErrorCode::Internal` is marked retryable, which
    /// makes JSON parse errors, programmer-error `Other(String)`, and the
    /// fall-through arm of the Delta classifier all incorrectly retryable.
    #[test]
    fn bug_1_4_json_parse_error_should_not_be_retryable() {
        let json_err: std::result::Result<serde_json::Value, _> = serde_json::from_str("{bad");
        let err = DeltaViewerError::Json(json_err.unwrap_err());
        assert!(
            !err.error_code().is_retryable(),
            "JSON parse errors are not transient — retrying will not make the bytes well-formed",
        );
    }

    /// review.md §1.4 — `Other(String)` is a programmer-error escape hatch;
    /// it should not be retryable either.
    #[test]
    fn bug_1_4_other_error_should_not_be_retryable() {
        let err = DeltaViewerError::Other("internal invariant violated".to_string());
        assert!(
            !err.error_code().is_retryable(),
            "Programmer-error 'Other' should not be retryable",
        );
    }
}
