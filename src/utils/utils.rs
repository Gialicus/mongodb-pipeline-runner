use std::{fs, path::Path};

use mongodb::bson::Document;
use serde_json::Value;

pub fn convert_bson_fields_to_json_fields(doc: Value) -> Value {
    // Verify is object
    match doc {
        Value::Object(mut obj) => {
            // iterate over field object
            for (_, value) in obj.iter_mut() {
                // check "$oid" in object
                if let Some(oid) = value.get("$oid") {
                    *value = Value::String(oid.as_str().unwrap().to_string());
                }
                // check "$date" in object
                else if let Some(date) = value.get("$date") {
                    *value = date.clone();
                }
            }
            Value::Object(obj)
        }
        // return fields if is not $oid or $date
        _ => doc,
    }
}

pub fn read_and_parse_pipeline(path: &str) -> Result<Vec<Document>, Box<dyn std::error::Error>> {
    let pipeline_content = fs::read_to_string(path)?;
    let pipeline_json: Value = serde_json::from_str(&pipeline_content)?;
    let pipeline: Vec<Document> = serde_json::from_value(pipeline_json)?;
    Ok(pipeline)
}

pub fn create_dir_if_not_exisist(output_path: &str) -> Result<&Path, Box<dyn std::error::Error>> {
    let stages_dir = Path::new(output_path);
    if !stages_dir.exists() {
        fs::create_dir(stages_dir)?;
    }
    Ok(stages_dir)
}
