use std::sync::Arc;

use arrow_schema::{DataType, Field};

pub fn format_rows(rows: u64) -> String {
    let mut result = rows.to_string();
    let mut i = result.len();
    while i > 3 {
        i -= 3;
        result.insert(i, ',');
    }
    result
}

pub(crate) fn get_stored_value(key: &str, default: &str) -> String {
    let window = web_sys::window().unwrap();
    let storage = window.local_storage().unwrap().unwrap();
    storage
        .get_item(key)
        .unwrap()
        .unwrap_or_else(|| default.to_string())
}

pub(crate) fn save_to_storage(key: &str, value: &str) {
    if let Some(window) = web_sys::window() {
        if let Ok(Some(storage)) = window.local_storage() {
            let _ = storage.set_item(key, value);
        }
    }
}

pub fn format_arrow_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "Boolean".to_string(),
        DataType::Utf8 => "String".to_string(),
        DataType::Struct(fields) => format_struct_type(fields),
        DataType::List(child) => format!("List<{}>", format_arrow_type(child.data_type())),
        _ => data_type.to_string(),
    }
}

pub fn format_struct_type(fields: &[Arc<Field>]) -> String {
    if fields.is_empty() {
        return "Struct{}".to_string();
    }

    let field_strs: Vec<String> = fields
        .iter()
        .map(|f| format!("{}: {}", f.name(), format_arrow_type(f.data_type())))
        .collect();

    format!("Struct{{{}}}", field_strs.join(", "))
}
