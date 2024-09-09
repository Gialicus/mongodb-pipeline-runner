use clap::Parser;
use mongodb::bson::{self, doc};
use mongodb::{bson::Document, Client, Collection};
use serde_json::Value;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

#[derive(Parser)]
#[command(name = "MongoDB Pipeline Runner")]
#[command(version = "1.0")]
#[command(about = "Run MongoDB pipeline and logs intermediate results", long_about = None)]
struct Cli {
    /// MongoDB URL
    #[arg(short, long)]
    url: String,

    /// Database name
    #[arg(short, long)]
    database: String,

    /// Collection name
    #[arg(short, long)]
    collection: String,

    /// JSON file with pipeline file path
    #[arg(short, long)]
    pipeline: String,

    /// Output Directory
    #[arg(short, long, default_value = "./stage_logs")]
    output_dir: String,

    /// Default results limit
    #[arg(short, long, default_value = "10")]
    limit: u32,
}

fn convert_bson_fields_to_json_fields(doc: Value) -> Value {
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

async fn log_intermediate_results(
    collection: &Collection<Document>,
    pipeline: Vec<Document>,
    output_path: &str,
    limit: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let stages_dir = Path::new(output_path);
    if !stages_dir.exists() {
        fs::create_dir(stages_dir)?;
    }

    for (i, _) in pipeline.iter().enumerate() {
        let mut current_pipeline = pipeline[0..=i].to_vec();
        current_pipeline.push(doc! { "$limit": limit });

        let mut cursor = collection.aggregate(current_pipeline).await?;
        let mut results = Vec::new();

        while cursor.advance().await? {
            let doc = cursor.deserialize_current()?;
            let value: Value = bson::to_bson(&doc)?.into();
            results.push(value);
        }

        let converted_results: Vec<Value> = results
            .into_iter()
            .map(convert_bson_fields_to_json_fields)
            .collect();

        let filename = stages_dir.join(format!("stage_{}_result.json", i + 1));
        let mut file = File::create(&filename)?;
        let json = serde_json::to_string_pretty(&converted_results)?;
        file.write_all(json.as_bytes())?;

        println!("Stage {} results saved to {:?}", i + 1, filename);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let pipeline_content = fs::read_to_string(&cli.pipeline)?;
    let pipeline_json: Value = serde_json::from_str(&pipeline_content)?;
    let pipeline: Vec<Document> = serde_json::from_value(pipeline_json)?;

    let uri = cli.url.as_str();
    let client = Client::with_uri_str(uri).await?;
    let db = client.database(&cli.database);
    let collection: Collection<Document> = db.collection(&cli.collection);

    log_intermediate_results(&collection, pipeline, &cli.output_dir, cli.limit).await?;

    Ok(())
}
