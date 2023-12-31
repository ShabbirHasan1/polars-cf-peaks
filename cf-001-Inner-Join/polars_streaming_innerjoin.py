import polars as pl
import os
import sys

master = pl.scan_csv("Inbox/Master.csv")
sample_df = master.fetch(10)
print(sample_df)

source_file_path = os.path.join("Inbox/", sys.argv[1])

fact_table = pl.scan_csv(source_file_path)

sample_df = fact_table.fetch(10)
print(sample_df)

print()
print("*** Processing: python polars_streaming_innerjoin.py ***")
print()

result = fact_table.join(master, on=["Product","Style"], 
                         how="inner").with_columns((
             pl.col("Quantity") * 
             pl.col("Unit_Price")).alias("Amount")).collect(streaming = True)            

result_file_path = f"Outbox/Polars_Streaming_Innerjoin_Result_{os.path.basename(source_file_path)}"

result.write_csv(result_file_path)

result_table = pl.scan_csv(result_file_path)

sample_df = result_table.fetch(10)
print(sample_df)

