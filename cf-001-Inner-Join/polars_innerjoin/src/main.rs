extern crate polars;
use polars::prelude::*;
use std::env;
use std::fs;

fn main() {
    let master_file_path = "Inbox/Master.csv";
    let master_df = match CsvReader::from_path(master_file_path).unwrap().finish() {
        Ok(df) => df,
        Err(e) => {
            println!("Error reading master file: {:?}", e);
            return;
        }
    }; 

    let source_file_path = match env::args().nth(1) {
        Some(path) => format!("Inbox/{}", path),
        None => {
            println!("Source file path argument missing");
            return;
        }
    };    

    let fact_table_df = match CsvReader::from_path(&source_file_path).unwrap().finish() {
        Ok(df) => df,
        Err(e) => {
            println!("Error reading source file: {:?}", e);
            return;
        }
    }; 

    let mut temp_df = match fact_table_df.join(&master_df, &["Product", "Style"], &["Product","Style"], JoinType::Inner, None) {
        Ok(df) => df,
        Err(e) => {
            println!("Error joining dataframes: {:?}", e);
            return;
        }
    };    
    
    let quantity = temp_df.column("Quantity").unwrap().f64().unwrap().clone();
    let unit_price = temp_df.column("Unit_Price").unwrap().f64().unwrap().clone();    

    let mut amount = (quantity * unit_price).into_series();
    amount.rename("Amount");
    
    let result_df = match temp_df.with_column(amount.clone()) {
        Ok(df) => df,
        Err(e) => {
            println!("Error adding new column: {:?}", e);
            return;
        }
    };
    
    let result_file_path = format!("Outbox/Polars_Rust_Innerjoin_Result-{}", source_file_path.split("/").last().unwrap()
    );  

    let file = match fs::File::create(&result_file_path) {
        Ok(file) => file,
        Err(e) => {
            println!("Error creating result file: {:?}", e);
            return;
        }
    };   
    
    match CsvWriter::new(file).has_header(true).finish(&result_df) {
        Ok(_) => (),
        Err(e) => {
            println!("Error writing to result file: {:?}", e);
            return;
        }
    };
  
}
