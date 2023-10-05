import sys
import time
import duckdb

start_time = time.time()

conn = duckdb.connect(database=':memory:', read_only=False)

conn.execute("CREATE TABLE master AS SELECT * FROM read_csv_auto('Inbox/Master.csv')")
conn.execute("CREATE VIEW filter_master AS SELECT * FROM master WHERE Style = 'F'")

conn.execute("CREATE TABLE fact_table AS SELECT * FROM read_csv_auto('Inbox/' || ?)", (sys.argv[1],))
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


detail_file_path = 'Outbox/DuckDB_Detail_Result_' + sys.argv[1]
summary_file_path = 'Outbox/DuckDB_Summary_Result_' + sys.argv[1]
conn.execute(f"COPY (SELECT * FROM detail_result) TO '{detail_file_path}' WITH (HEADER TRUE, FORMAT CSV)")
conn.execute(f"COPY (SELECT * FROM summary_result) TO '{summary_file_path}' WITH (HEADER TRUE, FORMAT CSV)")

detail_df = conn.execute("SELECT * FROM detail_result").fetch_df()
summary_df = conn.execute("SELECT * FROM summary_result").fetch_df()
print(detail_df.head(10))
print("\n")  
print(summary_df.head(10))
print("\n")  
end_time = time.time()

print("DuckDB CSV Duration (In Second): {}".format(round(end_time-start_time,3)))
