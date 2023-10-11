# polars-cf-peaks
Comparing Polars and Peaks in Query Syntax, Flexibility, Performance and Memory Utilization

## Current Script Folder: cf-002 InnerJoin-GroupBy

### Data: 1 Million Rows

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

### Data: 10 Million Rows

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

### Data: 100 Million Rows

| Test Case | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Average |
| --- | --- | --- | --- | --- | --- | --- |
| python polar-parquet.py | 0.4 | 0.47 | 0.45 | 0.47 | 0.45 | 0.45 |
| python duckdb-parquet.py | 0.87 | 0.93 | 0.89 | 0.97 | 0.96 | 0.92 |
| python polar-csv.py | 0.47 | 0.39 | 0.41 | 0.42 | 0.4 | 0.42 |
| python duckdb-csv.py | 1.27 | 1.15 | 1.24 | 1.13 | 1.19 | 1.2 |
| python peakrs.py | 0.27 | 0.22 | 0.21 | 0.23 | 0.22 | 0.23 |
| peakrs|  	0.18|  	0.16|  	0.17|  	0.14|  	0.15|  	0.16|
| peakcs|  	0.48|  	0.51|  	0.5|  	0.44|  	0.44|  	0.47|
| peakgo|  	0.22|  	0.18|  	0.2|  	0.18|  	0.17|  	0.19|

### Data: 1,000 Million Rows

| Test Case | Run 1 | Run 2 | Run 3 | Average |
| --- | --- | --- | --- | --- |
| python polar-parquet.py | 92.41 | 91.16 | 91.59 | 91.72 |
| python duckdb-parquet.py | 429.97 | 414.19 | 425.25 | 423.14 |
| peakrs|  	60.1|  	60.05|  	59.94|  	60.03|
| peakgo|  	36.53|  	31.3|  	32.13|  	33.32|



