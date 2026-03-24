mod protocol;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Mutex;

use delta_core::error::DeltaViewerError;
use deltalake::DeltaTable;
use lru::LruCache;
use protocol::{Command, Request, Response, ResponseBody, ResultPayload};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct TableCacheEntry {
    table: DeltaTable,
    row_count: Option<usize>,
}

static TABLE_CACHE: std::sync::LazyLock<Mutex<LruCache<(String, Option<i64>), TableCacheEntry>>> =
    std::sync::LazyLock::new(|| {
        Mutex::new(LruCache::new(NonZeroUsize::new(8).unwrap()))
    });

static COUNT_CACHE: std::sync::LazyLock<Mutex<HashMap<(String, Option<i64>), usize>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn error_response(id: String, e: DeltaViewerError) -> Response {
    let code = e.error_code();
    Response {
        id,
        body: ResponseBody::Error {
            error: e.to_string(),
            code,
            retryable: code.is_retryable(),
        },
    }
}

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Read error: {e}");
                break;
            }
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let request: Request = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Parse error: {e}");
                continue;
            }
        };

        let is_shutdown = matches!(request.command, Command::Shutdown {});

        let result = handle_request(request, &mut stdout).await;

        if let Some(response) = result {
            if let Err(e) = write_response(&mut stdout, &response).await {
                eprintln!("Write error: {e}");
                break;
            }
        }

        if is_shutdown {
            break;
        }
    }
}

const CHUNK_SIZE: usize = 200;

async fn write_response(
    stdout: &mut tokio::io::Stdout,
    response: &Response,
) -> std::io::Result<()> {
    let mut out = serde_json::to_string(response).unwrap();
    out.push('\n');
    stdout.write_all(out.as_bytes()).await?;
    stdout.flush().await
}

async fn handle_read_streaming(
    id: &str,
    result: delta_core::ReadResult,
    stdout: &mut tokio::io::Stdout,
) -> std::io::Result<()> {
    // Send header
    let header = Response {
        id: id.to_string(),
        body: ResponseBody::Result {
            result: ResultPayload::DataHeader {
                schema: result.schema,
                total_rows: result.total_rows,
                offset: result.offset,
                cdf_counts: result.cdf_counts,
            },
        },
    };
    write_response(stdout, &header).await?;

    // Send in chunks
    for (chunk_index, chunk) in result.rows.chunks(CHUNK_SIZE).enumerate() {
        let chunk_resp = Response {
            id: id.to_string(),
            body: ResponseBody::Result {
                result: ResultPayload::DataChunk {
                    rows: chunk.to_vec(),
                    chunk_index,
                },
            },
        };
        write_response(stdout, &chunk_resp).await?;
    }

    // Send done
    let total_sent = result.rows.len();
    let done = Response {
        id: id.to_string(),
        body: ResponseBody::Result {
            result: ResultPayload::DataDone { total_sent },
        },
    };
    write_response(stdout, &done).await?;

    Ok(())
}

/// Handles a request. Returns `None` if the response was already written (streaming).
async fn handle_request(req: Request, stdout: &mut tokio::io::Stdout) -> Option<Response> {
    let id = req.id.clone();

    match req.command {
        Command::ReadParquet { path, offset, limit } => {
            match delta_core::parquet::read_parquet(Path::new(&path), offset, limit) {
                Ok(result) => {
                    if let Err(e) = handle_read_streaming(&id, result, stdout).await {
                        eprintln!("Streaming write error: {e}");
                    }
                    None
                }
                Err(e) => Some(error_response(id, e)),
            }
        }
        Command::ReadDelta { path, offset, limit, version, known_total } => {
            match handle_read_delta(&path, offset, limit, version, known_total).await {
                Ok(result) => {
                    if let Err(e) = handle_read_streaming(&id, result, stdout).await {
                        eprintln!("Streaming write error: {e}");
                    }
                    None
                }
                Err(e) => Some(error_response(id, e)),
            }
        }
        Command::ReadCdf { path, start_version, end_version, offset, limit } => {
            match delta_core::delta::read_delta_cdf(
                Path::new(&path), start_version, end_version, offset, limit,
            ).await {
                Ok(result) => {
                    if let Err(e) = handle_read_streaming(&id, result, stdout).await {
                        eprintln!("Streaming write error: {e}");
                    }
                    None
                }
                Err(e) => Some(error_response(id, e)),
            }
        }
        Command::GetSchema { path } => {
            let p = Path::new(&path);
            let schema_result = if p.join("_delta_log").exists() {
                delta_core::delta::read_delta_schema(p).await
            } else {
                delta_core::parquet::read_parquet_schema(p)
            };

            Some(match schema_result {
                Ok(schema) => Response {
                    id,
                    body: ResponseBody::Result {
                        result: ResultPayload::Data {
                            schema,
                            rows: vec![],
                            total_rows: 0,
                            offset: 0,
                            limit: 0,
                        },
                    },
                },
                Err(e) => error_response(id, e),
            })
        }
        Command::GetHistory { path } => {
            Some(match delta_core::delta::get_delta_history(Path::new(&path)).await {
                Ok(result) => Response {
                    id,
                    body: ResponseBody::Result {
                        result: ResultPayload::from(result),
                    },
                },
                Err(e) => error_response(id, e),
            })
        }
        Command::GetTableInfo { path } => {
            Some(match delta_core::delta::get_delta_table_info(Path::new(&path)).await {
                Ok(result) => Response {
                    id,
                    body: ResponseBody::Result {
                        result: ResultPayload::from(result),
                    },
                },
                Err(e) => error_response(id, e),
            })
        }
        Command::RefreshTable { path } => {
            // Evict all cache entries for this path (all versions)
            {
                let mut cache = TABLE_CACHE.lock().unwrap();
                let keys: Vec<_> = cache
                    .iter()
                    .filter(|((p, _), _)| p == &path)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys {
                    cache.pop(&key);
                }
            }
            {
                let mut cache = COUNT_CACHE.lock().unwrap();
                cache.retain(|(p, _), _| p != &path);
            }
            Some(Response {
                id,
                body: ResponseBody::Result {
                    result: ResultPayload::Data {
                        schema: vec![],
                        rows: vec![],
                        total_rows: 0,
                        offset: 0,
                        limit: 0,
                    },
                },
            })
        }
        Command::Ping {} => Some(Response {
            id,
            body: ResponseBody::Result {
                result: ResultPayload::Data {
                    schema: vec![],
                    rows: vec![],
                    total_rows: 0,
                    offset: 0,
                    limit: 0,
                },
            },
        }),
        Command::Shutdown {} => Some(Response {
            id,
            body: ResponseBody::Result {
                result: ResultPayload::Data {
                    schema: vec![],
                    rows: vec![],
                    total_rows: 0,
                    offset: 0,
                    limit: 0,
                },
            },
        }),
    }
}

async fn handle_read_delta(
    path: &str,
    offset: usize,
    limit: usize,
    version: Option<i64>,
    known_total: Option<usize>,
) -> std::result::Result<delta_core::ReadResult, DeltaViewerError> {
    let cache_key = (path.to_string(), version);

    // Try to get table from cache
    let table = {
        let mut cache = TABLE_CACHE.lock().unwrap();
        cache.get(&cache_key).map(|e| e.table.clone())
    };

    let table = match table {
        Some(t) => t,
        None => {
            let t = delta_core::delta::load_delta_table(Path::new(path), version).await?;
            let mut cache = TABLE_CACHE.lock().unwrap();
            cache.put(
                cache_key.clone(),
                TableCacheEntry {
                    table: t.clone(),
                    row_count: None,
                },
            );
            t
        }
    };

    let mut result = delta_core::delta::query_delta_table(&table, offset, limit).await?;

    // Resolve total_rows: known_total > table cache > count cache > compute
    if let Some(total) = known_total {
        result.total_rows = total;
    } else {
        // Check count cache
        let cached_count = {
            let cache = COUNT_CACHE.lock().unwrap();
            cache.get(&cache_key).copied()
        };

        if let Some(count) = cached_count {
            result.total_rows = count;
        } else {
            // Check table cache entry
            let entry_count = {
                let mut cache = TABLE_CACHE.lock().unwrap();
                cache.get(&cache_key).and_then(|e| e.row_count)
            };

            if let Some(count) = entry_count {
                result.total_rows = count;
            } else {
                // Compute count
                let count = delta_core::delta::count_delta_rows(&table).await?;
                result.total_rows = count;

                // Store in both caches
                {
                    let mut cache = TABLE_CACHE.lock().unwrap();
                    if let Some(entry) = cache.get_mut(&cache_key) {
                        entry.row_count = Some(count);
                    }
                }
                {
                    let mut cache = COUNT_CACHE.lock().unwrap();
                    cache.insert(cache_key, count);
                }
            }
        }
    }

    Ok(result)
}
