use crate::engine::Executor;
use crate::storage::StorageEngine;
use crate::types::Value;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::json;
use std::sync::Arc;

#[napi(js_name = "LitePostgresNative")]
pub struct LitePostgresNative {
    executor: Arc<Mutex<Executor>>,
    #[allow(dead_code)]
    filepath: String,
}

#[napi]
impl LitePostgresNative {
    #[napi(constructor)]
    pub fn new(filepath: String) -> Result<Self> {
        let storage = StorageEngine::new(filepath.clone());
        let executor = Arc::new(Mutex::new(Executor::new(storage)));
        Ok(Self {
            executor,
            filepath,
        })
    }

    #[napi]
    pub fn exec(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<serde_json::Value> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        match exec.execute(&sql, &rust_params) {
            Ok(res) => Ok(json!({
                "rowCount": res.row_count,
                "command": res.command
            })),
            Err(e) => Err(Error::from_reason(e)),
        }
    }

    #[napi]
    pub fn exec2(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<serde_json::Value> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        match exec.execute(&sql, &rust_params) {
            Ok(res) => Ok(json!({
                "rows": res.rows,
                "rowCount": res.row_count,
                "fields": res.fields,
                "command": res.command
            })),
            Err(e) => Err(Error::from_reason(e)),
        }
    }

    #[napi]
    pub fn query(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<Vec<serde_json::Value>> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        match exec.execute(&sql, &rust_params) {
            Ok(res) => Ok(res.rows),
            Err(e) => Err(Error::from_reason(e)),
        }
    }

    #[napi]
    pub fn query2(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<serde_json::Value> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        match exec.execute(&sql, &rust_params) {
            Ok(res) => Ok(json!({
                "rows": res.rows,
                "rowCount": res.row_count,
                "fields": res.fields,
                "command": res.command
            })),
            Err(e) => Err(Error::from_reason(e)),
        }
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        let mut exec = self.executor.lock();
        exec.storage.flush();
        Ok(())
    }
}

fn convert_params(params: Option<Vec<serde_json::Value>>) -> Vec<Value> {
    match params {
        Some(list) => list
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
                serde_json::Value::String(s) => Value::Text(s),
                _ => Value::Text(v.to_string()),
            })
            .collect(),
        None => Vec::new(),
    }
}
