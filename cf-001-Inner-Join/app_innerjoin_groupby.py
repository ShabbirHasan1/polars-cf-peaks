import sys
import os
from datetime import datetime
import peakrs as pr
from pypeaks import run_batch, run_stream

df = pr.Dataframe()
df.log_file_name = "Outbox/PyPeakrs_Log_" + datetime.now().strftime("%y%m%d_%H%M%S") + ".csv"
pr.create_log(df)

pr.view_sample("Inbox/Master.csv")

source_file_path = os.path.join("Inbox/", sys.argv[1])

pr.view_sample(source_file_path)

df.partition_size_mb = 5
df.thread = 100

result_file_path = f"Outbox/Peakrs_Innerjoin_Groupby_Result_{os.path.basename(source_file_path)}"

"""
try:
    file = open(result_file_path, "w")
except:
    print("Fail to create file")"""

print("*** Processing: python peakrs_innerjoin.py ***")

query = [   
    ("build_key_value", "Product, Style => Table(key_value)"),      
]

master_df = run_batch(df, "Inbox/Master.csv", query)

query = [   
    ("filter", "Shop(S90..S99) Product(800..900)"),
    ("join_key_value", "Product, Style => Inner(key_value)"),
    ("add_column", "Quantity, Unit_Price => Multiply(Amount)"),   
    ("group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)"),   
    ("final_group_by", "Shop, Product => Sum(Count) Sum(Quantity) Sum(Amount)")
]

run_stream(df, source_file_path, master_df, query, result_file_path)

pr.view_sample(result_file_path)