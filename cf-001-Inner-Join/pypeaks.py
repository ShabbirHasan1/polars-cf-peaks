from time import time
import sys
import os
import math
from datetime import datetime
from pathlib import Path
import peakrs as pr

def run_batch(ref_df: pr.Dataframe, source_file_path: str, query: str) -> pr.Dataframe:    

    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)      
    df = pr.read_csv(ref_df, source_file_path)        

    i = 0

    while i < len(query):
        command, setting = query[i]
        command = command.lower()
        if command == "build_key_value":          
            df = pr.build_key_value(df, setting)             
            
        i += 1  

    return df

def run_stream(ref_df: pr.Dataframe, source_file_path: str, master_df: pr.Dataframe, query: str, result_file_path: str):    

    ref_df = pr.get_csv_partition_address(ref_df, source_file_path)
    total_batch_float = math.ceil(ref_df.partition_count / ref_df.thread)
    total_batch = int(total_batch_float)      

    print("Partition Count: ", ref_df.partition_count)
    print("Streaming Batch Count: ", total_batch)

    final_df_group = {}
    ref_df.processed_partition = 0
    ref_df.streaming_batch = 0  

    is_append_to_disk = True
    is_write_csv = False
    is_final_group_by = False
    is_final_distinct = False

    while ref_df.processed_partition < ref_df.partition_count:        

        df = pr.read_csv(ref_df, source_file_path)   

        i = 0

        while i < len(query):

            command, setting = query[i]
            command = command.lower()
          
            if command == "add_column":               
                df = pr.add_column(df, setting)               

            elif command == "distinct":               
                df = pr.distinct(df, setting)
                final_df_group[ref_df.streaming_batch] = df
                is_final_distinct = True
                is_write_csv = True
                is_append_to_disk = False

            elif command == "filter":               
                df = pr.filter(df, setting)
                is_append_to_disk = True

            elif command == "group_by":               
                df = pr.group_by(df, setting)
                final_df_group[ref_df.streaming_batch] = df
                is_final_group_by = True
                is_write_csv = True
                is_append_to_disk = False

            elif command == "final_distinct":                              
                is_write_csv = True

            elif command == "final_group_by":                              
                is_write_csv = True

            elif command == "join_key_value":                
                df = pr.join_key_value(df, master_df, setting)               
                
            i += 1  

        if is_write_csv == True: 
            if is_final_distinct == True:
                result_df = pr.final_distinct(final_df_group, setting)              
                pr.write_csv(result_df, result_file_path)         
            if is_final_group_by == True:
               result_df = pr.final_group_by(final_df_group, setting)              
               pr.write_csv(result_df, result_file_path)         

        if is_append_to_disk == True:
            pr.append_csv(df, result_file_path)

        ref_df.processed_partition += df.thread
        ref_df.streaming_batch += 1
        print(f"{ref_df.streaming_batch} ", end="")
        sys.stdout.flush()  