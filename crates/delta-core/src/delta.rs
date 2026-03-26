use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use deltalake::datafusion::logical_expr::LogicalPlanBuilder;
use deltalake::delta_datafusion::DataFusionMixins;
use deltalake::{DeltaTable, DeltaTableBuilder};

use crate::convert::batches_to_json_rows;
use crate::error::Result;
use crate::schema::{arrow_schema_to_columns, ColumnDef};
use crate::{HistoryEntry, HistoryResult, ReadResult, TableInfoResult};

/// Load a Delta table, optionally at a specific version.
pub async fn load_delta_table(path: &Path, version: Option<i64>) -> Result<DeltaTable> {
    let mut builder = DeltaTableBuilder::from_uri(path.to_string_lossy());
    if let Some(v) = version {
        builder = builder.with_version(v);
    }
    Ok(builder.load().await?)
}

/// Read schema from a Delta table.
pub async fn read_delta_schema(path: &Path) -> Result<Vec<ColumnDef>> {
    let table = load_delta_table(path, None).await?;
    let schema = table.snapshot()?.arrow_schema()?;
    Ok(arrow_schema_to_columns(&schema))
}

/// Count total rows in a Delta table using DataFusion.
pub async fn count_delta_rows(table: &DeltaTable) -> Result<usize> {
    let ctx = deltalake::datafusion::prelude::SessionContext::new();
    let provider = deltalake::datafusion::datasource::provider_as_source(Arc::new(table.clone()));
    let plan = LogicalPlanBuilder::scan("delta", provider, None)?.build()?;
    let df = ctx.execute_logical_plan(plan).await?;
    let count = df.count().await?;
    Ok(count)
}

/// Read paginated rows from an already-loaded Delta table.
pub async fn query_delta_table(
    table: &DeltaTable,
    offset: usize,
    limit: usize,
) -> Result<ReadResult> {
    let schema = table.snapshot()?.arrow_schema()?;
    let columns = arrow_schema_to_columns(&schema);

    let ctx = deltalake::datafusion::prelude::SessionContext::new();
    let provider = deltalake::datafusion::datasource::provider_as_source(Arc::new(table.clone()));
    let plan = LogicalPlanBuilder::scan("delta", provider, None)?.build()?;
    let df = ctx.execute_logical_plan(plan).await?;

    let page_df = df.limit(offset, Some(limit))?;
    let batches = page_df.collect().await?;
    let rows = batches_to_json_rows(&batches)?;

    Ok(ReadResult {
        schema: columns,
        rows,
        total_rows: 0, // caller should set this
        offset,
        limit,
        cdf_counts: None,
    })
}

/// Read paginated rows from a Delta table, optionally at a specific version.
/// Convenience wrapper that loads the table, counts rows, and queries.
pub async fn read_delta(
    path: &Path,
    offset: usize,
    limit: usize,
    version: Option<i64>,
) -> Result<ReadResult> {
    let table = load_delta_table(path, version).await?;

    let total_rows = count_delta_rows(&table).await?;

    let mut result = query_delta_table(&table, offset, limit).await?;
    result.total_rows = total_rows;

    Ok(result)
}

/// Read CDF (Change Data Feed) changes between two versions.
pub async fn read_delta_cdf(
    path: &Path,
    start_version: i64,
    end_version: i64,
    offset: usize,
    limit: usize,
) -> Result<ReadResult> {
    use deltalake::datafusion::functions_aggregate::expr_fn::count;
    use deltalake::datafusion::prelude::*;
    use deltalake::delta_datafusion::DeltaCdfTableProvider;

    let table = load_delta_table(path, Some(end_version)).await?;

    let cdf_builder = deltalake::operations::load_cdf::CdfLoadBuilder::new(
        table.log_store(),
        table.snapshot()?.clone(),
    )
    .with_starting_version(start_version)
    .with_ending_version(end_version);

    let provider = DeltaCdfTableProvider::try_new(cdf_builder)?;

    let ctx = SessionContext::new();
    let df = ctx.read_table(Arc::new(provider))?;

    // Compute counts by _change_type in a single aggregation
    let counts_df = df
        .clone()
        .aggregate(vec![col("_change_type")], vec![count(lit(1)).alias("cnt")])?;
    let count_batches = counts_df.collect().await?;

    let mut cdf_counts = crate::CdfCounts::default();
    let mut total_rows: usize = 0;
    for batch in &count_batches {
        let type_col = batch
            .column_by_name("_change_type")
            .unwrap()
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
            .unwrap();
        let cnt_col = batch
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let ct = type_col.value(i);
            let n = cnt_col.value(i) as usize;
            match ct {
                "insert" => {
                    cdf_counts.inserts += n;
                    total_rows += n;
                }
                "update_postimage" => {
                    cdf_counts.updates += n;
                    total_rows += n;
                }
                "delete" => {
                    cdf_counts.deletes += n;
                    total_rows += n;
                }
                _ => {} // update_preimage excluded from total
            }
        }
    }

    let schema = {
        let arrow_schema = df.schema().inner().clone();
        crate::schema::arrow_schema_to_columns(arrow_schema.as_ref())
    };

    let page_df = df.limit(offset, Some(limit))?;
    let batches = page_df.collect().await?;
    let rows = batches_to_json_rows(&batches)?;

    Ok(ReadResult {
        schema,
        rows,
        total_rows,
        offset,
        limit,
        cdf_counts: Some(cdf_counts),
    })
}

/// Get the commit history of a Delta table.
pub async fn get_delta_history(path: &Path) -> Result<HistoryResult> {
    let table = DeltaTableBuilder::from_uri(path.to_string_lossy())
        .load()
        .await?;

    let commits = table.history(None).await?;

    let entries = commits
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let operation_params = c.operation_parameters.as_ref().map(|params| {
                params
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect::<HashMap<String, String>>()
            });
            HistoryEntry {
                version: (commits.len() - 1 - i) as i64,
                timestamp: c.timestamp,
                operation: c.operation.clone(),
                operation_params,
                user_name: c.user_name.clone(),
            }
        })
        .collect();

    Ok(HistoryResult { entries })
}

/// Get metadata and properties of a Delta table.
pub async fn get_delta_table_info(path: &Path) -> Result<TableInfoResult> {
    let table = DeltaTableBuilder::from_uri(path.to_string_lossy())
        .load()
        .await?;

    let snapshot = table.snapshot()?;
    let metadata = snapshot.metadata();

    let name = metadata.name.clone();
    let description = metadata.description.clone();
    let partition_columns = metadata.partition_columns.clone();
    let created_time = metadata.created_time;
    let raw_configuration = metadata.configuration.clone();
    let configuration: HashMap<String, String> = raw_configuration
        .into_iter()
        .filter_map(|(k, v)| v.map(|val| (k, val)))
        .collect();

    let protocol = snapshot.protocol();
    let min_reader_version = protocol.min_reader_version;
    let min_writer_version = protocol.min_writer_version;

    let cdf_enabled = configuration
        .get("delta.enableChangeDataFeed")
        .map(|v| v == "true")
        .unwrap_or(false);

    // Compute file stats from snapshot
    let files = snapshot.file_actions()?;
    let num_files = files.len();
    let total_size_bytes: i64 = files.iter().map(|f| f.size).sum();

    let current_version = snapshot.version();
    let location = path.to_string_lossy().to_string();

    let id = metadata.id.clone();
    let format_provider = metadata.format.provider.clone();
    let format_options: HashMap<String, String> = metadata
        .format
        .options
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
        .collect();

    let reader_features = protocol
        .reader_features
        .as_ref()
        .map(|features| features.iter().map(|f| f.to_string()).collect::<Vec<_>>());
    let writer_features = protocol
        .writer_features
        .as_ref()
        .map(|features| features.iter().map(|f| f.to_string()).collect::<Vec<_>>());

    Ok(TableInfoResult {
        name,
        description,
        location,
        current_version,
        created_time,
        min_reader_version,
        min_writer_version,
        cdf_enabled,
        partition_columns,
        num_files,
        total_size_bytes,
        configuration,
        id,
        format_provider,
        format_options,
        reader_features,
        writer_features,
    })
}
