import sys
import os
from datetime import datetime
import peakrs as pr
from pypeaks import run_batch, run_stream

source_file_path = os.path.join("Inbox/", sys.argv[1])
result_file_path = f"Outbox/Peakrs_Innerjoin_Result_{os.path.basename(source_file_path)}"

try:
    file = open(result_file_path, "w")
except:
    print("Fail to create file")

query1 = [   
    ("build_key_value", "Product, Style => Table(key_value)"),   
]

query2 = [   
    ("join_key_value", "Product, Style => Inner(key_value)"),
    ("add_column", "Quantity, Unit_Price => Multiply(Amount)"),   
]

df = pr.Dataframe()
df.partition_size_mb = 5
df.thread = 100

master_df = run_batch(df, "Inbox/Master.csv", query1)
run_stream(df, source_file_path, master_df, query2, result_file_path)

pr.view_sample(result_file_path)