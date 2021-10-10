use crate::tx_engine::TransactionEngine;
use std::error::Error;

mod tx_engine;

/// Will output to stdout the CSV as desired. For performance in case of large inputs, or from TCP streams, this program uses asynchronous processing of CSVs
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args: Vec<String> = std::env::args().collect();

    // There should be two arguments, the first being the binary name (automatically passed) and the second being the input file (manually passed)
    if args.len() != 2 {
        panic!("Invalid number of arguments. Expected an input file with no additional arguments");
    }

    let input_file = args.remove(1);
    let output = tokio::io::stdout();

    TransactionEngine::process_file(input_file, output).await
}
