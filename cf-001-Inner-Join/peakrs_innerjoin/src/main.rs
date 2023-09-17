extern crate chrono;
extern crate rayon;
extern crate peakrs; 
use peakrs::*;
use chrono::Local;
use std::collections::HashMap;
use std::path::Path;
use io::Error;
use std::fs::File;
use std::io::{self, Write};

fn main() -> Result<(), Error> {

    let start_time = Local::now();      

    let mut df = Dataframe {
        end_column_count: 0,
        validate_row: 0,
        estimate_row: 0,
        is_line_br_13_exist: false,
        is_line_br_10_exist: false,       
        column_name: Vec::new(),
        column_name_end_address: 0,        
        file_size: 0,       
        keyvalue_table: HashMap::new(),
        partition_address: Vec::new(),
        partition_count: 0,
        delimiter: 0,        
        error_message: String::new(),
        alt_column_name: Vec::new(),
        command_name: String::new(),
        command_setting: String::new(),
        duration_second: 0.0,       
        end_partition_count: 1,
        end_row_count: 0,
        end_time: Local::now(),       
        log_file_name: format!("Outbox/Peakrs_Log_{}.csv", start_time.format("%y%m%d_%H%M%S")),
        partition_data: Vec::new(),
        partition_size_mb: 0,
        processed_partition: 0,
        start_column_count: 0,
        start_partition_count: 0,
        start_row_count: 0,
        start_time: Local::now(),
        streaming_batch: 0,
        thread: 0,
        value_column_name: Vec::new(),      
        vector_table: Vec::new(),
        vector_table_group: HashMap::new(),
    };

    create_log(&df);

    let source_file_path = get_cli_file_path("10M-Fact.csv"); 

    view_sample(&source_file_path);   
    df.partition_size_mb = 5;
    df.thread = 100;  

    let result_file_path = &format!("Outbox/Peakrs_Rust_Innerjoin_Result_{}", Path::new(&source_file_path).file_name().unwrap().to_str().unwrap());            
    
    match File::create(result_file_path) {
        Ok(_file) => {
            // handle file
        }
        Err(_) => {
            println!("Fail to create file");
        }
    }

    innerjoin(df, &source_file_path,  &result_file_path);    
   
    view_sample(&result_file_path);   

    return Ok(());
}


fn innerjoin(ref_df: Dataframe, source_file_path: &str, result_file_path: &str) {  

    let mut ref_df = get_csv_partition_address(&ref_df, "Inbox/Master.csv");          
    let mut master_df = read_csv(&ref_df, "Inbox/Master.csv");        
    master_df = build_key_value(&master_df, "Product, Style => Table(KeyValue)");    

    ref_df = get_csv_partition_address(&ref_df, source_file_path);   

    let total_batch_float = (ref_df.partition_count as f64 / ref_df.thread as f64).ceil();
    let total_batch = total_batch_float as usize;
    println!("Partition Count: {}", ref_df.partition_count);
    println!("Streaming Batch Count: {}", total_batch);
    
    ref_df.processed_partition = 0;
    ref_df.streaming_batch = 0;

    while &ref_df.processed_partition < &ref_df.partition_count {             
       
        let df = read_csv(&ref_df, source_file_path);      
		let df = join_key_value(&df, &master_df, "Product, Style => Inner(KeyValue)");
        let df = add_column(&df, "Quantity, Unit_Price => Multiply(Amount)");	
      
        append_csv(&df, &result_file_path);

        ref_df.processed_partition += df.thread;    
        ref_df.streaming_batch += 1;
        print!("{} ", ref_df.streaming_batch);
        io::stdout().flush().unwrap();        
    }
}