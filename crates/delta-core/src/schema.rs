use std::collections::HashMap;

use arrow::datatypes::{DataType, Schema};
use serde::{Deserialize, Serialize};

/// Definition of a single column, serializable for the VS Code frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Human-readable data type string (e.g., "Int64", "List<Struct<x: Int32>>").
    pub data_type: String,
    /// Whether the column allows null values.
    pub nullable: bool,
    /// Arbitrary key-value metadata attached to the column.
    pub metadata: HashMap<String, String>,
}

/// Converts an Arrow [`Schema`] into a list of [`ColumnDef`]s.
pub fn arrow_schema_to_columns(schema: &Schema) -> Vec<ColumnDef> {
    schema
        .fields()
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: format_data_type(f.data_type()),
            nullable: f.is_nullable(),
            metadata: f.metadata().clone(),
        })
        .collect()
}

/// Formats an Arrow [`DataType`] into a human-readable string, recursing into
/// nested types like `List`, `Struct`, and `Map`.
fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::List(f) => format!("List<{}>", format_data_type(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_data_type(f.data_type())))
                .collect();
            format!("Struct<{}>", inner.join(", "))
        }
        DataType::Map(f, _) => {
            // The Map's child field is a Struct<key, value>; surface those types
            // directly instead of leaking the wrapping struct.
            match f.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => format!(
                    "Map<{}, {}>",
                    format_data_type(fields[0].data_type()),
                    format_data_type(fields[1].data_type()),
                ),
                other => format!("Map<{}>", format_data_type(other)),
            }
        }
        other => format!("{other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_arrow_schema_to_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, false),
        ]);
        let cols = arrow_schema_to_columns(&schema);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "Int64");
        assert!(!cols[0].nullable);
        assert!(cols[0].metadata.is_empty());
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].data_type, "Utf8");
        assert!(cols[1].nullable);
        assert_eq!(cols[2].data_type, "Float64");
        assert!(cols[2].metadata.is_empty());
    }

    #[test]
    fn test_format_data_type_nested() {
        // List<Struct<x: Int32, y: Utf8>>
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Utf8, true),
            ]
            .into(),
        );
        let list_type = DataType::List(Arc::new(Field::new("item", struct_type, true)));
        assert_eq!(
            format_data_type(&list_type),
            "List<Struct<x: Int32, y: Utf8>>"
        );
    }

    #[test]
    fn test_format_data_type_simple() {
        assert_eq!(format_data_type(&DataType::Boolean), "Boolean");
        assert_eq!(format_data_type(&DataType::Int32), "Int32");
    }

    // -----------------------------------------------------------------------
    // Failing tests added to cover bugs documented in /review.md.
    // -----------------------------------------------------------------------

    /// review.md §4.1 — `Map` formatting recurses into the wrapping struct
    /// rather than rendering `Map<KeyType, ValueType>`. The current output is
    /// `Map<Struct<keys: Utf8, values: Int64>>`, which is leaky and verbose.
    #[test]
    fn bug_4_1_map_format_includes_key_and_value_types() {
        let entries_struct = DataType::Struct(
            vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int64, true),
            ]
            .into(),
        );
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", entries_struct, false)),
            false,
        );
        assert_eq!(
            format_data_type(&map_type),
            "Map<Utf8, Int64>",
            "Map formatting should expose key and value types, not the internal entries struct",
        );
    }
}
