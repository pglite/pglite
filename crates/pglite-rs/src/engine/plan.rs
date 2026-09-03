use crate::types::{FieldInfo, Value};

#[derive(Clone, Debug)]
pub enum OperandTemplate {
    Param(usize), // 0-based parameter index ($1 -> 0)
    Literal(Value),
    Number(f64),
}

#[derive(Clone, Debug)]
pub enum ColOpTemplate {
    Eq(OperandTemplate),
    NotEq(OperandTemplate),
    LowerEq(OperandTemplate),
    UpperEq(OperandTemplate),
    Gt(OperandTemplate),
    Lt(OperandTemplate),
    Gte(OperandTemplate),
    Lte(OperandTemplate),
    Between(OperandTemplate, OperandTemplate),
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug)]
pub struct ConditionTemplate {
    pub col_idx: usize,
    pub op: ColOpTemplate,
}

#[derive(Clone, Debug)]
pub enum PlannedProjectedExpr {
    PrimaryCol { col_idx: usize, alias: String },
    JoinedCol { col_idx: usize, alias: String },
    CoalescePrimaryCol { col_idx: usize, default_val: Value, alias: String },
}

#[derive(Clone, Debug)]
pub struct PlannedJoin {
    pub joined_table_name: String,
    pub primary_join_col_idx: usize,
    pub joined_join_col_idx: usize,
    pub is_pk_join: bool,
}

#[derive(Clone, Debug)]
pub enum UpdateValTemplate {
    CurrentTimestamp,
    Param(usize),
    Literal(Value),
}

#[derive(Clone, Debug)]
pub enum ExecutionPlan {
    PointLookupPk {
        table_name: String,
        param_idx: Option<usize>,
        literal_pk: Option<i64>,
        fields: Vec<FieldInfo>,
    },
    PointLookupString {
        table_name: String,
        col_idx: usize,
        param_idx: Option<usize>,
        literal_str: Option<String>,
        fields: Vec<FieldInfo>,
    },
    GeneralSelect {
        table_name: String,
        join: Option<PlannedJoin>,
        conditions: Vec<ConditionTemplate>,
        order_by: Option<(usize, bool)>, // (col_idx, is_desc)
        limit: Option<OperandTemplate>,
        offset: Option<OperandTemplate>,
        projections: Vec<PlannedProjectedExpr>,
        fields: Vec<FieldInfo>,
    },
    Update {
        table_name: String,
        updates: Vec<(usize, UpdateValTemplate)>,
        conditions: Vec<ConditionTemplate>,
        pk_target_param: Option<usize>,
        pk_target_literal: Option<i64>,
        has_returning: bool,
        fields: Vec<FieldInfo>,
    },
}

#[inline(always)]
pub fn resolve_operand(opnd: &OperandTemplate, params: &[Value]) -> Value {
    match opnd {
        OperandTemplate::Param(idx) => params.get(*idx).cloned().unwrap_or(Value::Null),
        OperandTemplate::Literal(v) => v.clone(),
        OperandTemplate::Number(n) => Value::Float(*n),
    }
}

#[inline(always)]
pub fn resolve_number(opnd: &OperandTemplate, params: &[Value]) -> f64 {
    match opnd {
        OperandTemplate::Param(idx) => {
            params.get(*idx).and_then(|v| v.as_f64()).unwrap_or(0.0)
        }
        OperandTemplate::Literal(v) => v.as_f64().unwrap_or(0.0),
        OperandTemplate::Number(n) => *n,
    }
}

#[inline(always)]
pub fn evaluate_planned_condition(row: &[Value], cond: &ConditionTemplate, params: &[Value]) -> bool {
    let val = match row.get(cond.col_idx) {
        Some(v) => v,
        None => return false,
    };

    match &cond.op {
        ColOpTemplate::Eq(opnd) => {
            let target = resolve_operand(opnd, params);
            val == &target
        }
        ColOpTemplate::NotEq(opnd) => {
            let target = resolve_operand(opnd, params);
            val != &target
        }
        ColOpTemplate::LowerEq(opnd) => {
            let target_str = resolve_operand(opnd, params).as_text().unwrap_or_default().to_lowercase();
            val.as_text().map(|s| s.to_lowercase() == target_str).unwrap_or(false)
        }
        ColOpTemplate::UpperEq(opnd) => {
            let target_str = resolve_operand(opnd, params).as_text().unwrap_or_default().to_uppercase();
            val.as_text().map(|s| s.to_uppercase() == target_str).unwrap_or(false)
        }
        ColOpTemplate::Gt(opnd) => {
            let target = resolve_number(opnd, params);
            val.as_f64().map(|n| n > target).unwrap_or(false)
        }
        ColOpTemplate::Lt(opnd) => {
            let target = resolve_number(opnd, params);
            val.as_f64().map(|n| n < target).unwrap_or(false)
        }
        ColOpTemplate::Gte(opnd) => {
            let target = resolve_number(opnd, params);
            val.as_f64().map(|n| n >= target).unwrap_or(false)
        }
        ColOpTemplate::Lte(opnd) => {
            let target = resolve_number(opnd, params);
            val.as_f64().map(|n| n <= target).unwrap_or(false)
        }
        ColOpTemplate::Between(a, b) => {
            let min = resolve_number(a, params);
            let max = resolve_number(b, params);
            val.as_f64().map(|n| n >= min && n <= max).unwrap_or(false)
        }
        ColOpTemplate::IsNull => val.is_null(),
        ColOpTemplate::IsNotNull => !val.is_null(),
    }
}

pub fn project_row_planned(
    primary_row: &[Value],
    joined_row: Option<&[Value]>,
    projections: &[PlannedProjectedExpr],
) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(projections.len());
    for p in projections {
        match p {
            PlannedProjectedExpr::PrimaryCol { col_idx, alias } => {
                let v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                map.insert(alias.clone(), value_to_json(v));
            }
            PlannedProjectedExpr::JoinedCol { col_idx, alias } => {
                let v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                map.insert(alias.clone(), value_to_json(v));
            }
            PlannedProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                let v = match primary_row.get(*col_idx) {
                    Some(val) if !val.is_null() => val,
                    _ => default_val,
                };
                map.insert(alias.clone(), value_to_json(v));
            }
        }
    }
    serde_json::Value::Object(map)
}

#[inline(always)]
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s.clone()),
    }
}
