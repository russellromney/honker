use serde_json::{json, Value};
use std::collections::HashMap;

const CATALOG: &str = include_str!("../surface.json");

fn as_int(value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or_else(|| format!("expected int, got {value}")),
        Value::String(s) => s.parse().map_err(|e| format!("{e}")),
        _ => Err(format!("expected int, got {value}")),
    }
}

fn as_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn resolve(token: &Value, prefix: &str, vars: &HashMap<String, Value>) -> Result<Value, String> {
    let Some(s) = token.as_str() else {
        return Ok(token.clone());
    };
    if let Some(name) = s.strip_prefix("$ns:") {
        return Ok(Value::String(format!("{prefix}_{name}")));
    }
    if let Some(keys) = s.strip_prefix("$json:") {
        let ids: Result<Vec<i64>, String> = keys
            .split(',')
            .map(|k| as_int(vars.get(k).ok_or_else(|| format!("missing {k}"))?))
            .collect();
        return Ok(Value::String(serde_json::to_string(&ids?).unwrap()));
    }
    if let Some(name) = s.strip_prefix('$') {
        return vars
            .get(name)
            .cloned()
            .ok_or_else(|| format!("missing {name}"));
    }
    Ok(Value::String(s.to_string()))
}

fn resolve_text(text: &str, prefix: &str, vars: &HashMap<String, Value>) -> String {
    let mut out = text.to_string();
    for (k, v) in vars {
        out = out.replace(&format!("${k}"), &as_text(v));
    }
    out.replace("$ns:", &format!("{prefix}_"))
}

fn check(
    expect: &Value,
    result: &Value,
    prefix: &str,
    vars: &HashMap<String, Value>,
) -> Result<(), String> {
    let kind = expect["kind"].as_str().ok_or("missing kind")?;
    match kind {
        "int_gt" => {
            if as_int(result)? <= expect["n"].as_i64().unwrap() {
                return Err(format!("got {result}"));
            }
        }
        "int_eq" => {
            if as_int(result)? != expect["n"].as_i64().unwrap() {
                return Err(format!("got {result}"));
            }
        }
        "int_ge" => {
            if as_int(result)? < expect["n"].as_i64().unwrap() {
                return Err(format!("got {result}"));
            }
        }
        "int_gt_ref" => {
            let refn = as_int(&vars[expect["ref"].as_str().unwrap()])?;
            if as_int(result)? <= refn {
                return Err(format!("got {result}"));
            }
        }
        "eq_ref" => {
            let refn = as_int(&vars[expect["ref"].as_str().unwrap()])?;
            if as_int(result)? != refn {
                return Err(format!("got {result}"));
            }
        }
        "json_len" => {
            let parsed: Value = serde_json::from_str(&as_text(result)).map_err(|e| e.to_string())?;
            if parsed.as_array().map(Vec::len) != Some(expect["n"].as_u64().unwrap() as usize) {
                return Err(format!("got {result}"));
            }
        }
        "json_id_eq_ref" => {
            let parsed: Value = serde_json::from_str(&as_text(result)).map_err(|e| e.to_string())?;
            let arr = parsed.as_array().ok_or_else(|| format!("got {result}"))?;
            let refn = as_int(&vars[expect["ref"].as_str().unwrap()])?;
            if arr.len() != 1 || as_int(&arr[0]["id"])? != refn {
                return Err(format!("got {result}"));
            }
        }
        "contains" => {
            let needle = resolve_text(expect["s"].as_str().unwrap(), prefix, vars);
            if !as_text(result).contains(&needle) {
                return Err(format!("{needle} not in {result}"));
            }
        }
        "empty_string" => {
            if !as_text(result).is_empty() {
                return Err(format!("expected empty string, got {result}"));
            }
        }
        "is_null" => {
            if !result.is_null() {
                return Err(format!("expected NULL, got {result}"));
            }
        }
        other => return Err(format!("unknown expect kind {other}")),
    }
    Ok(())
}

#[allow(dead_code)]
fn run_catalog(
    prefix: &str,
    mut scalar: impl FnMut(&str, Vec<Value>) -> Result<Value, String>,
) -> Result<(), String> {
    let catalog: Value = serde_json::from_str(CATALOG).map_err(|e| e.to_string())?;
    let mut vars: HashMap<String, Value> = HashMap::new();
    for step in catalog["steps"].as_array().unwrap() {
        let sql = step["sql"].as_str().unwrap();
        let mut args = Vec::new();
        for arg in step["args"].as_array().unwrap() {
            args.push(resolve(arg, prefix, &vars)?);
        }
        let id = step["id"].as_str().unwrap();
        let result = scalar(sql, args).map_err(|e| format!("{id} failed: {e}"))?;
        if let Some(store) = step["store"].as_str() {
            vars.insert(store.to_string(), result.clone());
        }
        if !step["expect"].is_null() {
            check(&step["expect"], &result, prefix, &vars).map_err(|e| format!("{id}: {e}"))?;
        }
    }
    Ok(())
}
