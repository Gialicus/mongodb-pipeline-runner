pub mod types;
pub mod utils;

use clap::Parser;
use mongodb::bson::{self, doc};
use mongodb::{bson::Document, Collection};
use serde_json::Value;
use std::fs::File;
use std::io::Write;
use types::cli::Cli;
use utils::utils::{
    convert_bson_fields_to_json_fields, create_dir_if_not_exisist, get_mongodb_collection,
    read_and_parse_pipeline,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let pipeline: Vec<Document> = read_and_parse_pipeline(&cli.pipeline)?;

    let collection: Collection<Document> = get_mongodb_collection(&cli).await?;

    log_intermediate_results(&collection, pipeline, &cli.output_dir, cli.limit).await?;

    Ok(())
}

async fn log_intermediate_results(
    collection: &Collection<Document>,
    pipeline: Vec<Document>,
    output_path: &str,
    limit: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let stages_dir = create_dir_if_not_exisist(output_path)?;

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
