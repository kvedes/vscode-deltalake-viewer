use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

use crate::error::Result;

/// Convert a slice of RecordBatches to JSON row objects.
pub fn batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<Map<String, Value>>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut map = Map::new();
            let schema = batch.schema();
            for col_idx in 0..batch.num_columns() {
                let field = schema.field(col_idx);
                let col = batch.column(col_idx);
                let value = array_value_to_json(col, row_idx);
                map.insert(field.name().clone(), value);
            }
            rows.push(map);
        }
    }
    Ok(rows)
}

fn array_value_to_json(array: &dyn Array, idx: usize) -> Value {
    if array.is_null(idx) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(idx))
        }
        DataType::Int8 => json_number!(array, Int8Array, idx),
        DataType::Int16 => json_number!(array, Int16Array, idx),
        DataType::Int32 => json_number!(array, Int32Array, idx),
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt8 => json_number!(array, UInt8Array, idx),
        DataType::UInt16 => json_number!(array, UInt16Array, idx),
        DataType::UInt32 => json_number!(array, UInt32Array, idx),
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Float16 => {
            let arr = array.as_any().downcast_ref::<Float16Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx).to_f64())
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Value::String(format!("<{} bytes>", arr.value(idx).len()))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Value::String(arr.value_as_date(idx).map_or_else(
                || "null".to_string(),
                |d| d.to_string(),
            ))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Value::String(arr.value_as_datetime(idx).map_or_else(
                || "null".to_string(),
                |d| d.to_string(),
            ))
        }
        DataType::Timestamp(_, _) => {
            // Use the display representation which includes timezone handling
            Value::String(format!("{}", array_to_string(array, idx)))
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = arr.value(idx);
            let items: Vec<Value> = (0..values.len())
                .map(|i| array_value_to_json(&values, i))
                .collect();
            Value::Array(items)
        }
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = Map::new();
            for (i, field) in arr.fields().iter().enumerate() {
                let col = arr.column(i);
                map.insert(field.name().clone(), array_value_to_json(col, idx));
            }
            Value::Object(map)
        }
        _ => Value::String(format!("{}", array_to_string(array, idx))),
    }
}

fn array_to_string(array: &dyn Array, idx: usize) -> String {
    arrow::util::display::array_value_to_string(array, idx).unwrap_or_else(|_| "?".to_string())
}

macro_rules! json_number {
    ($array:expr, $arr_type:ty, $idx:expr) => {{
        let arr = $array.as_any().downcast_ref::<$arr_type>().unwrap();
        Value::Number(arr.value($idx).into())
    }};
}
use json_number;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_batches_to_json_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();

        let rows = batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], Value::Number(1.into()));
        assert_eq!(rows[0]["name"], Value::String("alice".to_string()));
        assert_eq!(rows[0]["active"], Value::Bool(true));
        assert_eq!(rows[1]["active"], Value::Bool(false));
    }

    #[test]
    fn test_batches_to_json_nulls() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ],
        )
        .unwrap();

        let rows = batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows[0]["b"], Value::Null);
        assert_eq!(rows[1]["a"], Value::Null);
    }

    #[test]
    fn test_batches_to_json_nested() {
        let inner_field = Field::new("item", DataType::Int32, true);
        let list_field = Field::new("vals", DataType::List(Arc::new(inner_field)), true);
        let struct_field = Field::new(
            "s",
            DataType::Struct(vec![Field::new("x", DataType::Utf8, false)].into()),
            true,
        );
        let schema = Schema::new(vec![list_field, struct_field]);

        let list_arr = {
            let values = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
            let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3].into());
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                offsets,
                Arc::new(values),
                None,
            )
            .unwrap()
        };

        let struct_arr = StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["hello", "world"])) as _,
        )]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_arr), Arc::new(struct_arr)])
                .unwrap();

        let rows = batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows[0]["vals"], Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())]));
        assert_eq!(rows[0]["s"]["x"], Value::String("hello".to_string()));
    }

    #[test]
    fn test_batches_to_json_timestamps() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        let ts_arr = TimestampMillisecondArray::from(vec![1_700_000_000_000i64]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ts_arr)]).unwrap();

        let rows = batches_to_json_rows(&[batch]).unwrap();
        // Should be a string representation
        assert!(rows[0]["ts"].is_string());
    }

    #[test]
    fn test_batches_to_json_binary() {
        let schema = Schema::new(vec![Field::new("bin", DataType::Binary, false)]);
        let bin_arr = BinaryArray::from(vec![&[1u8, 2, 3][..]]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(bin_arr)]).unwrap();

        let rows = batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows[0]["bin"], Value::String("<3 bytes>".to_string()));
    }
}
