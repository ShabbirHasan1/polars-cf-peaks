# polars-cf-peaks
Comparing Polars and Peaks in Query Syntax, Flexibility, Performance and Memory Utilization

## Current Script Folder: cf-002 InnerJoin-GroupBy
## Current Version: Polars (0.19.7), DuckDB (0.9), Python (3.11)
## Current Query

The run_batch function will store the result table in memory, but a limitation is that the dataset size cannot exceed the available memory.
On the other hand, when run_batch outputs the result table to disk, it has the advantage of handling datasets that are larger than the available memory.

```
query1 = query(
    "filter", "Style(=F)",
    "build_key_value", "Product, Style => Table(key_value)")

master_df = run_batch(df, "Inbox/Master.csv", query1)

query2 = query(    
    "filter", "Shop(S90..S99)",
    "join_key_value", "Product, Style => Inner(key_value)",    
    "add_column", "Quantity, Unit_Price => Multiply(Amount)",
    "filter", "Amount:Float(>100000)",
    "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)"
)

source_file = os.path.join("Inbox/", sys.argv[1])
result_file = [f"Outbox/Peakpy-Detail-Result-{os.path.basename(source_file)}", 
               f"Outbox/Peakpy-Summary-Result-{os.path.basename(source_file)}"]

run_stream(df, source_file, master_df, query2, result_file)

```


## All Benchmarking Results Are Recorded in Seconds

### Scale: 1 Million Rows  
### Video: https://youtu.be/cgqLdb_RXLc

| Test Case | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Average |
| --- | --- | --- | --- | --- | --- | --- |
| python polar-parquet.py | 0.4 | 0.47 | 0.45 | 0.47 | 0.45 | 0.45 |
| python duckdb-parquet.py | 0.87 | 0.93 | 0.89 | 0.97 | 0.96 | 0.92 |
| python polar-csv.py | 0.47 | 0.39 | 0.41 | 0.42 | 0.4 | 0.42 |
| python duckdb-csv.py | 1.27 | 1.15 | 1.24 | 1.13 | 1.19 | 1.2 |
| python peakrs.py | 0.27 | 0.22 | 0.21 | 0.23 | 0.22 | 0.23 |
| peakrs | 0.18 | 0.16 | 0.17 | 0.14 | 0.15 | 0.16 |
| peakcs | 0.48 | 0.51 | 0.5 | 0.44 | 0.44 | 0.47 |
| peakgo|  	0.22|  	0.18|  	0.2|  	0.18|  	0.17|  	0.19|

### Scale: 10 Million Rows
### Video: https://youtu.be/cgqLdb_RXLc

| Test Case | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Average |
| --- | --- | --- | --- | --- | --- | --- |
| python polar-parquet.py | 1.25 | 1.15 | 1.14 | 1.1 | 1.06 | 1.14 |
| python duckdb-parquet.py | 1.44 | 1.39 | 1.36 | 1.36 | 1.29 | 1.37 |
| python polar-csv.py | 1.01 | 1.1 | 1.07 | 1.04 | 1.05 | 1.05 |
| python duckdb-csv.py | 2.26 | 2.19 | 2.17 | 2.16 | 2.15 | 2.19 |
| python peakrs.py | 0.79 | 0.76 | 0.76 | 0.71 | 0.73 | 0.75 |
| peakrs|  	0.68|  	0.7|  	0.68|  	0.68|  	0.73|  	0.69|
| peakcs|  	2.01|  	1.96|  	2.01|  	1.96|  	2.01|  	1.99|
| peakgo|  	0.73|  	0.69|  	0.69|  	0.7|  	0.68|  	0.7|

### Scale: 100 Million Rows
### Video: https://youtu.be/cgqLdb_RXLc

| Test Case | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Average |
| --- | --- | --- | --- | --- | --- | --- |
| python polar-parquet.py | 8.43 | 7.84 | 8.1 | 7.91 | 8.27 | 8.11 |
| python duckdb-parquet.py | 9.45 | 6.11 | 6.36 | 6.34 | 6.31 | 6.91 |
| python polar-csv.py | 6.8 | 6.83 | 6.83 | 6.83 | 6.82 | 6.82 |
| python duckdb-csv.py | 14.53 | 13.02 | 12.96 | 12.93 | 12.96 | 13.28 |
| python peakrs.py | 5.17 | 5.18 | 5.08 | 5.17 | 5.07 | 5.13 |
| peakrs | 5.15 | 5.11 | 4.88 | 5.07 | 4.97 | 5.04 |
| peakcs |13.67|13.47|14.27|13.45|13.41|13.65|
| peakgo|3.62|3.61|3.46|3.97|3.57|3.65|

### Scale: 1 Billion Rows
### Video: https://youtu.be/egGCBMppVL8


| Test Case | Run 1 | Run 2 | Run 3 | Average |
| --- | --- | --- | --- | --- |
| python polar-parquet.py | 92.41 | 91.16 | 91.59 | 91.72 |
| python duckdb-parquet.py | 429.97 | 414.19 | 425.25 | 423.14 |
| peakrs|  	60.1|  	60.05|  	59.94|  	60.03|
| peakgo|  	36.53|  	31.3|  	32.13|  	33.32|

### Scale: 10 Billion Rows
### Peakrs Video: https://youtu.be/Y2yJtWfgAq0
### Peakgo Video: https://youtu.be/un8Y7Y0Cd9Q

| Test Case | Run 1 | Run 2 | Average |
|-----------|-------|-------|---------|
| peakrs    | 611.4 |613.71 | 612.56  |
| peakgo    |397.78 |414.25 | 406.02  |

### Automatic Benchmarking Software: https://youtu.be/Uohm_-zr_sc
### How-to Expand Dataset: https://youtu.be/eXwqamjWbjU

### Other Comparisons

| | Polars | Peaks |
| --- | --- | --- |
| Author | Ritchie Vink | Max Yu |
| Start Project | Year 2020 | Year 2023 |
| Contributor | Many | One |
| Current Project Nature | Business | Hobby for Retirement |
| Language |  |  |
| - Library | Rust | Rust, Go, C-Sharp |
|  | (Open Source) | (Proprierary) |
| - App | Python, Rust, R | Python, Rust, Go, C-Sharp |
|  | (Open Source) | (Open Source) |
| Supports Streaming | Yes | Yes |
| Memory Utilization | Very High | Very Little |




