use crate::engine::Executor;
use crate::storage::StorageEngine;
use crate::types::Value;
use napi::bindgen_prelude::*;
use napi::{JsFunction, JsObject, JsUnknown};
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
                "success": true,
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
                "success": true,
                "rows": res.rows,
                "rowCount": res.row_count,
                "fields": res.fields,
                "command": res.command
            })),
            Err(e) => Err(Error::from_reason(e)),
        }
    }

    #[napi]
    pub fn query_json(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<String> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        exec.execute_rows_json(&sql, &rust_params).map_err(Error::from_reason)
    }

    #[napi]
    pub fn query2_json(&self, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<String> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        exec.execute_full_json(&sql, &rust_params).map_err(Error::from_reason)
    }

    #[napi]
    pub fn query(&self, env: Env, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<JsUnknown> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        let json_str = exec.execute_rows_json(&sql, &rust_params).map_err(Error::from_reason)?;
        let global = env.get_global()?;
        let json_obj: JsObject = global.get_named_property("JSON")?;
        let parse_fn: JsFunction = json_obj.get_named_property("parse")?;
        let js_str = env.create_string(&json_str)?;
        parse_fn.call(None, &[js_str.into_unknown()])
    }

    #[napi]
    pub fn query2(&self, env: Env, sql: String, params: Option<Vec<serde_json::Value>>, _db_name: Option<String>) -> Result<JsUnknown> {
        let rust_params = convert_params(params);
        let mut exec = self.executor.lock();
        let json_str = exec.execute_full_json(&sql, &rust_params).map_err(Error::from_reason)?;
        let global = env.get_global()?;
        let json_obj: JsObject = global.get_named_property("JSON")?;
        let parse_fn: JsFunction = json_obj.get_named_property("parse")?;
        let js_str = env.create_string(&json_str)?;
        parse_fn.call(None, &[js_str.into_unknown()])
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        let mut exec = self.executor.lock();
        exec.storage.flush();
        Ok(())
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
