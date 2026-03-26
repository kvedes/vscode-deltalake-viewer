use std::path::Path;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::convert::batches_to_json_rows;
use crate::error::Result;
use crate::schema::{arrow_schema_to_columns, ColumnDef};
use crate::ReadResult;

/// Read schema from a Parquet file.
pub fn read_parquet_schema(path: &Path) -> Result<Vec<ColumnDef>> {
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = reader.schema();
    Ok(arrow_schema_to_columns(schema))
}

/// Read paginated rows from a Parquet file.
/// Uses native offset/limit on the reader to avoid materializing the entire file.
pub fn read_parquet(path: &Path, offset: usize, limit: usize) -> Result<ReadResult> {
    // Get total rows from metadata — no data scan needed
    let file = std::fs::File::open(path)?;
    let metadata_reader = SerializedFileReader::new(file)?;
    let total_rows = metadata_reader.metadata().file_metadata().num_rows() as usize;

    // Read only the needed rows using native offset/limit
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let columns = arrow_schema_to_columns(&schema);

    let reader = builder
        .with_batch_size(limit)
        .with_offset(offset)
        .with_limit(limit)
        .build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    let rows = batches_to_json_rows(&batches)?;

    Ok(ReadResult {
        schema: columns,
        rows,
        total_rows,
        offset,
        limit,
        cdf_counts: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn write_test_parquet() -> NamedTempFile {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    None,
                    Some("dave"),
                    Some("eve"),
                ])),
            ],
        )
        .unwrap();

        let tmp = NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tmp
    }

    #[test]
    fn test_read_parquet_schema() {
        let tmp = write_test_parquet();
        let cols = read_parquet_schema(tmp.path()).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn test_read_parquet_pagination() {
        let tmp = write_test_parquet();
        let result = read_parquet(tmp.path(), 1, 2).unwrap();
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.offset, 1);
    }
}
