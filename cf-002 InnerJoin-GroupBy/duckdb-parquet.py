import duckdb
import sys
import time

start_time = time.time()

conn = duckdb.connect(database=':memory:', read_only=False)

conn.execute("CREATE TABLE master AS SELECT * FROM parquet_scan('Inbox/Master.parquet')")
conn.execute("CREATE VIEW filter_master AS SELECT * FROM master WHERE Style = 'F'")

conn.execute("CREATE TABLE fact_table AS SELECT * FROM parquet_scan('Inbox/' || ?)", (sys.argv[1],))
conn.execute("""
    CREATE VIEW detail_result AS 
    SELECT fact_table.*, Quantity * Unit_Price AS Amount 
    FROM fact_table 
    JOIN filter_master ON fact_table.Product = filter_master.Product AND fact_table.Style = filter_master.Style
    WHERE Shop >= 'S90' AND Shop <= 'S99' AND Amount > 100000
""")

conn.execute("""
    CREATE VIEW summary_result AS 
    SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount 
    FROM detail_result 
    GROUP BY Shop, Product
""")

detail_df = conn.execute("SELECT * FROM detail_result").fetch_df()
summary_df = conn.execute("SELECT * FROM summary_result").fetch_df()
detail_df.to_parquet('Outbox/DuckDB_Detail_Result_' + sys.argv[1].replace('.parquet', '') + '.parquet')
summary_df.to_parquet('Outbox/DuckDB_Summary_Result_' + sys.argv[1].replace('.parquet', '') + '.parquet')

print(detail_df.head(10))
print("\n")  
print(summary_df.head(10))
print("\n")  
end_time = time.time()
print("DuckDB Parquet Duration (In Second): {}".format(round(end_time-start_time,3)))