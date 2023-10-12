import duckdb
import sys
import time

start_time = time.time()

# Create a persistent database on disk
conn = duckdb.connect()

# Execute SQL queries using duckdb.sql()
duckdb.sql("CREATE TABLE master AS SELECT * FROM parquet_scan('Inbox/Master.parquet')")
duckdb.sql("CREATE VIEW filter_master AS SELECT * FROM master WHERE Style >= 'B' AND Style <= 'F'")

# Use Python's f-string for string formatting
duckdb.sql(f"CREATE TABLE fact_table AS SELECT * FROM parquet_scan('Inbox/{sys.argv[1]}')")

duckdb.sql("""
    CREATE VIEW detail_result AS
    SELECT fact_table.*, Quantity * Unit_Price AS Amount
    FROM fact_table
    JOIN filter_master ON fact_table.Product = filter_master.Product AND fact_table.Style = filter_master.Style
    WHERE Shop >= 'S21' AND Shop <= 'S99' AND Amount > 10000
""")

duckdb.sql("""
    CREATE VIEW summary_result AS
    SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount
    FROM detail_result
    GROUP BY Shop, Product
""")

# Use fetchdf() instead of fetch_df()
detail_df = duckdb.sql("SELECT * FROM detail_result").fetchdf()
summary_df = duckdb.sql("SELECT * FROM summary_result").fetchdf()
detail_df.to_parquet('Outbox/DuckDB_Detail_Result_' + sys.argv[1].replace('.parquet', '') + '.parquet')
summary_df.to_parquet('Outbox/DuckDB_Summary_Result_' + sys.argv[1].replace('.parquet', '') + '.parquet')

print(detail_df.head(10))
print("\n")  
print(summary_df.head(10))
print("\n")  
end_time = time.time()
print("DuckDB Parquet Duration (In Second): {}".format(round(end_time-start_time,3)))
