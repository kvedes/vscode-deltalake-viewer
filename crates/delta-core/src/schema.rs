use std::collections::HashMap;

use arrow::datatypes::{DataType, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

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
        DataType::Map(f, _) => format!("Map<{}>", format_data_type(f.data_type())),
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
}
