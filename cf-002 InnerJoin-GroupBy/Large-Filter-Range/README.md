# polars-cf-peaks
Comparing Polars and Peaks in Query Syntax, Flexibility, Performance and Memory Utilization

## Very Large Filter Range Benchmarks for the Same InnerJoin-GroupBy Setting

### Polars Process Parquet Compare Peaks Process CSV
### Script Folder: https://github.com/hkpeaks/polars-cf-peaks/tree/main/cf-002%20InnerJoin-GroupBy/Large-Filter-Range

```
query1 = query(
    "filter", "Style(B..F)",
    "build_key_value", "Product, Style => Table(key_value)")

master_df = run_batch(df, "Inbox/Master.csv", query1)

query2 = query(    
    "filter", "Shop(S21..S99)",
    "join_key_value", "Product, Style => Inner(key_value)",    
    "add_column", "Quantity, Unit_Price => Multiply(Amount)",
    "filter", "Amount:Float(>1000)",
    "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)"
)

source_file = os.path.join("Inbox/", sys.argv[1])
result_file = [f"Outbox/Peakpy-Detail-Result-{os.path.basename(source_file)}", 
               f"Outbox/Peakpy-Summary-Result-{os.path.basename(source_file)}"]

run_stream(df, source_file, master_df, query2, result_file)

```

### Scale: 1 Million Rows
### Video: https://youtu.be/EwAtKrLzki8

Note: peakrs - Rust, peakcs - C#, peakgo - Golang


| Test Case             | Run 1 | Run 2 | Run 3 | Average |
|-----------------------|-------|-------|-------|---------|
| python polar-parquet.py | 1.07  | 1.0   | 1.0   | 1.02    |
| python peakrs.py        | 0.87  | 0.87  | 0.96  | 0.9     |
| peakrs                  | 0.89  | 0.95  | 0.92  | 0.92    |
| peakcs                  | 2.2   | 1.76  | 1.68  | 1.88    |
| peakgo                  | 0.82  | 0.57  | 0.56  | 0.65    |

### Scale: 10 Million Rows
### Video: https://youtu.be/EwAtKrLzki8

| Test Case             | Run 1 | Run 2 | Run 3 | Average |
|-----------------------|-------|-------|-------|---------|
| python polar-parquet.py | 6.18  | 5.97  | 5.95  | 6.03    |
| python peakrs.py        | 3.69  | 4.14  | 3.82  | 3.88    |
| peakrs                  | 3.35  | 3.18  | 3.34  | 3.29    |
| peakcs                  | 11.3   | 10.62 | 11.03 | 10.98   |
| peakgo                  | 2.94   | 3.27   | 3.17   | 3.13    |

### Scale: 100 Million Rows
### Video: https://youtu.be/EwAtKrLzki8

| Test Case               | Run 1 | Run 2 | Run 3 | Average |
|-------------------------|-------|-------|-------|---------|
| python polar-parquet.py | 58.08 | 62.51 | 55.38 | 58.66   |
| python peakrs.py        | 26.5  | 26.98 | 25.67 | 26.38   |
| peakrs                  | 29.73 | 28.78 | 24.25 | 27.59   |
| peakcs                  | 96.49 | 100.1 | 91.66 | 96.08   |
| peakgo                  | 21.63 | 22.18 | 21.51 | 21.77   |

### Scale: 1,000 Million Rows
### Video: https://youtu.be/EwAtKrLzki8

| Test Case               | Run 1  | Run 2  | Run 3  | Average |
|-------------------------|--------|--------|--------|---------|
| python polar-parquet.py | 551.88 | 567.05 | 566.19 | 561.71  |
| python peakrs.py        | 338.34 | 343.25 | 351.75 | 344.45  |
| peakrs                  | 275.16 | 286.67 | 292.25 | 284.69  |
| peakcs                  | 913.86 | 954.7  | 950.1  | 939.55  |
| peakgo                  | 214.52 | 212.75 | 216.88 | 214.72  |

## Automatic Benchmarking Software: https://youtu.be/Uohm_-zr_sc
## How-to Expand Dataset: https://youtu.be/eXwqamjWbjU

## Other Comparisons

| | Polars | Peaks |
| --- | --- | --- |
| Author | Ritchie Vink | Max Yu |
| Start Project | Year 2020 | Year 2023 |
| Contributor | Many | One |
| Current Project Nature | Business | Hobby for Retirement |
| Language |  |  |
| - Dataframe Library | Rust | Rust, Go, C-Sharp |
|  | (Open Source) | (Proprierary) |
| - App | Python, Rust, R | Python, Rust, Go, C-Sharp |
|  | (Open Source) | (Open Source) |
| Supports Streaming | Yes | Yes |
| Memory Utilization | Very High | Very Little |





