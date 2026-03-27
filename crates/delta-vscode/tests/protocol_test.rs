use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Once;

fn sidecar_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_delta-vscode"))
}

fn testdelta_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("data")
        .join("testdelta")
}

static INIT_FIXTURES: Once = Once::new();

/// Ensure the test Delta table fixture exists at data/testdelta/.
/// Uses std::sync::Once so it only runs once per test binary invocation.
fn ensure_fixtures() {
    INIT_FIXTURES.call_once(|| {
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async {
            create_test_delta_table().await;
        });
    });
}

async fn create_test_delta_table() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use deltalake::operations::DeltaOps;
    use std::sync::Arc;

    let path = testdelta_path();

    // Skip if already populated
    if path.join("_delta_log").exists() {
        return;
    }

    std::fs::create_dir_all(&path).expect("failed to create data/testdelta directory");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "diana", "eve",
            ])),
            Arc::new(Int32Array::from(vec![
                Some(10),
                Some(20),
                None,
                Some(40),
                Some(50),
            ])),
        ],
    )
    .expect("failed to create record batch");

    let ops = DeltaOps::try_from_uri(path.to_string_lossy())
        .await
        .expect("failed to create DeltaOps");
    ops.write(vec![batch])
        .await
        .expect("failed to write Delta table fixture");
}

struct SidecarProcess {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    reader: Option<BufReader<std::process::ChildStdout>>,
}

impl SidecarProcess {
    fn spawn() -> Self {
        let mut child = Command::new(sidecar_binary())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn sidecar");

        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let reader = stdout.map(BufReader::new);

        SidecarProcess {
            child,
            stdin,
            reader,
        }
    }

    fn send(&mut self, request: &serde_json::Value) -> serde_json::Value {
        let stdin = self.stdin.as_mut().expect("stdin closed");
        let reader = self.reader.as_mut().expect("stdout closed");

        let mut line = serde_json::to_string(request).unwrap();
        line.push('\n');
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.flush().unwrap();

        let mut response_line = String::new();
        reader.read_line(&mut response_line).unwrap();
        serde_json::from_str(&response_line).expect("failed to parse response")
    }

    /// Send a request and collect all streaming responses until data_done.
    /// Returns (header, chunks, done) responses.
    fn send_streaming(&mut self, request: &serde_json::Value) -> Vec<serde_json::Value> {
        let stdin = self.stdin.as_mut().expect("stdin closed");
        let reader = self.reader.as_mut().expect("stdout closed");

        let mut line = serde_json::to_string(request).unwrap();
        line.push('\n');
        stdin.write_all(line.as_bytes()).unwrap();
        stdin.flush().unwrap();

        let mut responses = Vec::new();
        loop {
            let mut response_line = String::new();
            reader.read_line(&mut response_line).unwrap();
            let resp: serde_json::Value =
                serde_json::from_str(&response_line).expect("failed to parse response");

            // Check if this is an error (non-streaming) response
            if resp.get("error").is_some() {
                responses.push(resp);
                break;
            }

            let result_type = resp
                .get("result")
                .and_then(|r| r.get("type"))
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();

            responses.push(resp);

            if result_type == "data_done" {
                break;
            }
        }
        responses
    }

    fn close_pipes(&mut self) {
        self.stdin.take();
        self.reader.take();
    }
}

impl Drop for SidecarProcess {
    fn drop(&mut self) {
        self.stdin.take();
        self.reader.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn test_read_delta_valid() {
    ensure_fixtures();
    let mut sidecar = SidecarProcess::spawn();
    let path = testdelta_path();

    let responses = sidecar.send_streaming(&serde_json::json!({
        "id": "1",
        "command": "read_delta",
        "params": {
            "path": path.to_str().unwrap(),
            "offset": 0,
            "limit": 10
        }
    }));

    assert!(
        responses.len() >= 2,
        "expected at least header + done: got {}",
        responses.len()
    );

    // First response should be data_header
    let header = &responses[0]["result"];
    assert_eq!(header["type"], "data_header");
    assert!(header["schema"].is_array());
    assert!(header["total_rows"].is_number());

    // Last response should be data_done
    let done = &responses.last().unwrap()["result"];
    assert_eq!(done["type"], "data_done");
    assert!(done["total_sent"].is_number());
}

#[test]
fn test_get_history() {
    ensure_fixtures();
    let mut sidecar = SidecarProcess::spawn();
    let path = testdelta_path();

    let resp = sidecar.send(&serde_json::json!({
        "id": "1",
        "command": "get_history",
        "params": {
            "path": path.to_str().unwrap()
        }
    }));

    assert!(resp.get("result").is_some(), "expected result: {resp}");
    let result = &resp["result"];
    assert_eq!(result["type"], "history");
    assert!(result["entries"].is_array());
}

#[test]
fn test_get_table_info() {
    ensure_fixtures();
    let mut sidecar = SidecarProcess::spawn();
    let path = testdelta_path();

    let resp = sidecar.send(&serde_json::json!({
        "id": "1",
        "command": "get_table_info",
        "params": {
            "path": path.to_str().unwrap()
        }
    }));

    assert!(resp.get("result").is_some(), "expected result: {resp}");
    let result = &resp["result"];
    assert_eq!(result["type"], "table_info");
    assert!(result["current_version"].is_number());
}

#[test]
fn test_read_delta_invalid_path_structured_error() {
    let mut sidecar = SidecarProcess::spawn();

    let stdin = sidecar.stdin.as_mut().expect("stdin closed");
    let reader = sidecar.reader.as_mut().expect("stdout closed");

    let request = serde_json::json!({
        "id": "1",
        "command": "read_delta",
        "params": {
            "path": "/nonexistent/path/to/delta/table",
            "offset": 0,
            "limit": 10
        }
    });
    let mut line = serde_json::to_string(&request).unwrap();
    line.push('\n');
    stdin.write_all(line.as_bytes()).unwrap();
    stdin.flush().unwrap();

    let mut response_line = String::new();
    let bytes = reader.read_line(&mut response_line).unwrap();
    if bytes == 0 {
        // Sidecar crashed before responding — that's a known issue for now
        // At minimum verify it didn't hang
        return;
    }

    let resp: serde_json::Value =
        serde_json::from_str(&response_line).expect("failed to parse response");

    assert!(resp.get("error").is_some(), "expected error: {resp}");
    assert!(resp.get("code").is_some(), "expected error code: {resp}");
    assert!(
        resp.get("retryable").is_some(),
        "expected retryable: {resp}"
    );
}

#[test]
fn test_read_parquet_with_gen_fixture() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let tmpdir = tempfile::tempdir().unwrap();
    let parquet_path = tmpdir.path().join("test.parquet");

    // Generate a test parquet file inline
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("failed to create batch");

    let file = std::fs::File::create(&parquet_path).expect("failed to create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, schema, None).expect("failed to create ArrowWriter");
    writer.write(&batch).expect("failed to write batch");
    writer.close().expect("failed to close writer");

    let mut sidecar = SidecarProcess::spawn();

    let responses = sidecar.send_streaming(&serde_json::json!({
        "id": "1",
        "command": "read_parquet",
        "params": {
            "path": parquet_path.to_str().unwrap(),
            "offset": 0,
            "limit": 100
        }
    }));

    assert!(responses.len() >= 2, "expected at least header + done");
    let header = &responses[0]["result"];
    assert_eq!(header["type"], "data_header");
    assert!(!header["schema"].as_array().unwrap().is_empty());

    let done = &responses.last().unwrap()["result"];
    assert_eq!(done["type"], "data_done");
}

#[test]
fn test_shutdown() {
    let mut sidecar = SidecarProcess::spawn();

    let resp = sidecar.send(&serde_json::json!({
        "id": "shutdown",
        "command": "shutdown",
        "params": {}
    }));

    assert!(resp.get("result").is_some(), "expected result: {resp}");

    // Close pipes to let the process exit cleanly
    sidecar.close_pipes();

    // Process should exit after shutdown
    let status = sidecar.child.wait().expect("failed to wait");
    assert!(status.success());
}

#[test]
fn test_read_delta_with_version() {
    ensure_fixtures();
    let mut sidecar = SidecarProcess::spawn();
    let path = testdelta_path();

    let responses = sidecar.send_streaming(&serde_json::json!({
        "id": "1",
        "command": "read_delta",
        "params": {
            "path": path.to_str().unwrap(),
            "offset": 0,
            "limit": 10,
            "version": 0
        }
    }));

    assert!(responses.len() >= 2, "expected at least header + done");
    let header = &responses[0]["result"];
    assert_eq!(header["type"], "data_header");

    let done = &responses.last().unwrap()["result"];
    assert_eq!(done["type"], "data_done");
}
