from time import time
import sys
import os
import math
from datetime import datetime
from pathlib import Path
import peakrs as pr

def get_filename(base_name, extension):
    counter = 0
    while True:
        if counter == 0:
            file_name = f"{base_name}.{extension}"
        else:
            file_name = f"{base_name}{counter}.{extension}"
        if not os.path.exists(file_name):
            return file_name
        counter += 1

def join_table(ref_df: pr.Dataframe, source_file_path: str, result_file_path: str):    
    
    ref_df = pr.get_csv_partition_address(ref_df, "Inbox/Master.csv")      
    master_df = pr.read_csv(ref_df, "Inbox/Master.csv")      
    master_df = pr.build_key_value(master_df, "Product, Style => Table(KeyValue)")
    
    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)
    total_batch_float = math.ceil(ref_df.partition_count / ref_df.thread)
    total_batch = int(total_batch_float)      
    print("Partition Count: ", ref_df.partition_count)
    print("Streaming Batch Count: ", total_batch)

    ref_df.processed_partition = 0
    ref_df.streaming_batch = 0   

    while ref_df.processed_partition < ref_df.partition_count:        
       
        df = pr.read_csv(ref_df, source_file_path)      
        df = pr.join_key_value(df, master_df, "Product, Style => Inner(KeyValue)")    
        df = pr.add_column(df, "Quantity, Unit_Price => Multiply(Amount)")

        pr.append_csv(df, result_file_path)
        
        ref_df.processed_partition += df.thread
        ref_df.streaming_batch += 1
        print(f"{ref_df.streaming_batch} ", end="")
        sys.stdout.flush()           

  

df = pr.Dataframe()
df.log_file_name = "Outbox/PyPeakrs_Log_" + datetime.now().strftime("%y%m%d_%H%M%S") + ".csv"
pr.create_log(df)

pr.view_sample("Inbox/Master.csv")

source_file_path = os.path.join("Inbox/", sys.argv[1])

pr.view_sample(source_file_path)

df.partition_size_mb = 5
df.thread = 100

result_file_path = f"Outbox/Peakrs_Innerjoin_Result_{os.path.basename(source_file_path)}"

try:
    file = open(result_file_path, "w")
except:
    print("Fail to create file")

join_table(df, source_file_path, result_file_path)

pr.view_sample(result_file_path)
