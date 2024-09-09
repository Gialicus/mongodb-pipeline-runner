## MongoDB pipeline runner

Run MongoDB aggregate pipeline and logs intermediate results

```
Options:
  -u, --url <URL>                MongoDB URL
  -d, --database <DATABASE>      Database name
  -c, --collection <COLLECTION>  Collection name
  -p, --pipeline <PIPELINE>      JSON file with pipeline path
  -o, --output-dir <OUTPUT_DIR>  Output Directory [default: ./stage_logs]
  -l, --limit <LIMIT>            Default results limit [default: 10]
  -h, --help                     Print help
  -V, --version                  Print version
```

#### Example

```
cargo run -- -u "mongodb://localhost:27017" -d "test_db" -c "test_collection" -p "test.json"
```
