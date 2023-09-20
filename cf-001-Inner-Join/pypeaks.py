from time import time
import sys
import os
import math
from datetime import datetime
from pathlib import Path
import peakrs as pr

def run_batch(ref_df: pr.Dataframe, source_file_path: str, query: str) -> pr.Dataframe:    
    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)      
    master_df = pr.read_csv(ref_df, source_file_path)        
    i = 0
    while i < len(query):
        command, setting = query[i]
        command = command.lower()
        if command == "build_key_value":          
            master_df = pr.build_key_value(master_df, setting)     
        i += 1    
    return master_df

def run_stream(ref_df: pr.Dataframe, source_file_path: str, master_df: pr.Dataframe, query: str, result_file_path: str):    
    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)
    total_batch_float = math.ceil(ref_df.partition_count / ref_df.thread)
    total_batch = int(total_batch_float)      
    print("Partition Count: ", ref_df.partition_count)
    print("Streaming Batch Count: ", total_batch)
    ref_df.processed_partition = 0
    ref_df.streaming_batch = 0   
    while ref_df.processed_partition < ref_df.partition_count:        
        df = pr.read_csv(ref_df, source_file_path)   
        i = 0
        while i < len(query):
            command, setting = query[i]
            command = command.lower()
            if command == "join_key_value":                
                df = pr.join_key_value(df, master_df, setting)    
            elif command == "add_column":               
                df = pr.add_column(df, setting)
            elif command == "filter":               
                df = pr.add_column(df, setting)
            i += 1              
        pr.append_csv(df, result_file_path)
        ref_df.processed_partition += df.thread
        ref_df.streaming_batch += 1
        print(f"{ref_df.streaming_batch} ", end="")
        sys.stdout.flush()  
