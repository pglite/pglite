use crate::engine::Executor;
use crate::storage::StorageEngine;
use crate::types::Value;
use serde_json::json;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct PGliteWasm {
    executor: Executor,
    #[allow(dead_code)]
    filepath: String,
}

#[wasm_bindgen]
impl PGliteWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(filepath: Option<String>) -> Result<PGliteWasm, JsValue> {
        let path = filepath.unwrap_or_else(|| ":memory:".to_string());
        let storage = StorageEngine::new(path.clone());
        let executor = Executor::new(storage);
        Ok(Self {
            executor,
            filepath: path,
        })
    }

    #[wasm_bindgen]
    pub fn exec(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let rust_params = parse_params(params)?;
        match self.executor.execute(sql, &rust_params) {
            Ok(res) => {
                let v = json!({
                    "success": true,
                    "rowCount": res.row_count,
                    "command": res.command
                });
                to_js_value(&v)
            }
            Err(e) => Err(JsValue::from_str(&e)),
        }
    }

    #[wasm_bindgen]
    pub fn exec2(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let rust_params = parse_params(params)?;
        match self.executor.execute(sql, &rust_params) {
            Ok(res) => {
                let v = json!({
                    "success": true,
                    "rows": res.rows,
                    "rowCount": res.row_count,
                    "fields": res.fields,
                    "command": res.command
                });
                to_js_value(&v)
            }
            Err(e) => Err(JsValue::from_str(&e)),
        }
    }

    #[wasm_bindgen]
    pub fn query_json(&mut self, sql: &str, params: JsValue) -> Result<String, JsValue> {
        let rust_params = parse_params(params)?;
        self.executor
            .execute_rows_json(sql, &rust_params)
            .map_err(|e| JsValue::from_str(&e))
    }

    #[wasm_bindgen]
    pub fn query2_json(&mut self, sql: &str, params: JsValue) -> Result<String, JsValue> {
        let rust_params = parse_params(params)?;
        self.executor
            .execute_full_json(sql, &rust_params)
            .map_err(|e| JsValue::from_str(&e))
    }

    #[wasm_bindgen]
    pub fn query(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let json_str = self.query_json(sql, params)?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e}")))?;
        to_js_value(&parsed)
    }

    #[wasm_bindgen]
    pub fn query2(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsValue> {
        let json_str = self.query2_json(sql, params)?;
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| JsValue::from_str(&format!("JSON parse error: {e}")))?;
        to_js_value(&parsed)
    }

    #[wasm_bindgen]
    pub fn flush(&mut self) {
        self.executor.storage.flush();
    }

    #[wasm_bindgen]
    pub fn close(&mut self) {
        self.executor.storage.flush();
    }
}

fn to_js_value<T: serde::Serialize + ?Sized>(val: &T) -> Result<JsValue, JsValue> {
    val.serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|e| JsValue::from_str(&format!("Serialization error: {e}")))
}

fn parse_params(params: JsValue) -> Result<Vec<Value>, JsValue> {
    if params.is_null() || params.is_undefined() {
        return Ok(Vec::new());
    }

    let json_vals: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(params)
        .map_err(|e| JsValue::from_str(&format!("Invalid parameters format: {e}")))?;

    Ok(json_vals
        .into_iter()
        .map(|v| match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::text(s),
            _ => Value::text(v.to_string()),
        })
        .collect())
}
