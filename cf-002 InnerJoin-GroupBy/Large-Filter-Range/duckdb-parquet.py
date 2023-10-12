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
    WHERE Shop >= 'S21' AND Shop <= 'S99' AND Amount > 1000
""")

duckdb.sql("""
    CREATE VIEW summary_result AS
    SELECT Shop, Product, COUNT(*) AS Count, SUM(Quantity) AS Sum_Quantity, SUM(Amount) AS Sum_Amount
    FROM detail_result
    GROUP BY Shop, Product
""")

# Save the detail_result and summary_result to parquet files directly using duckdb.sql() instead of fetchdf()
detail_filename = f"Outbox/DuckDB_Detail_Result_{sys.argv[1].replace('.parquet', '')}.parquet"
summary_filename = f"Outbox/DuckDB_Summary_Result_{sys.argv[1].replace('.parquet', '')}.parquet"
duckdb.sql(f"COPY (SELECT * FROM detail_result) TO '{detail_filename}' (FORMAT PARQUET)")
duckdb.sql(f"COPY (SELECT * FROM summary_result) TO '{summary_filename}' (FORMAT PARQUET)")

end_time = time.time()
print("DuckDB Parquet Duration (In Second): {}".format(round(end_time-start_time,3)))



"""
D:\Benchmarking>python duckdb-parquet.py 1M_Fact.parquet
DuckDB Parquet Duration (In Second): 0.173

D:\Benchmarking>python duckdb-parquet.py 10M_Fact.parquet
DuckDB Parquet Duration (In Second): 0.8

D:\Benchmarking>python duckdb-parquet.py 100M_Fact.parquet
DuckDB Parquet Duration (In Second): 9.035

D:\Benchmarking>python duckdb-parquet.py 1000M_Fact.parquet
DuckDB Parquet Duration (In Second): 573.928  """

