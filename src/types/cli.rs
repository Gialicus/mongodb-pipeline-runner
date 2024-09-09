use clap::Parser;

#[derive(Parser)]
#[command(name = "MongoDB Pipeline Runner")]
#[command(version = "0.0.1")]
#[command(about = "Run MongoDB aggregate pipeline and logs intermediate results", long_about = None)]
pub struct Cli {
    /// MongoDB URL
    #[arg(short, long)]
    pub url: String,

    /// Database name
    #[arg(short, long)]
    pub database: String,

    /// Collection name
    #[arg(short, long)]
    pub collection: String,

    /// JSON file with pipeline path
    #[arg(short, long)]
    pub pipeline: String,

    /// Output Directory
    #[arg(short, long, default_value = "./stage_logs")]
    pub output_dir: String,

    /// Default results limit
    #[arg(short, long, default_value = "10")]
    pub limit: u32,
}
