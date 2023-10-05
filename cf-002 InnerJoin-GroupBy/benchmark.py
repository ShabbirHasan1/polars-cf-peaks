import os
import subprocess
from datetime import datetime
import csv
import argparse
import re

# Handle command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('data_file', nargs='?', default='1M_Fact')
parser.add_argument('batch_run', nargs='?', default=5, type=int)
args = parser.parse_args()

data_file = args.data_file
batch_run = args.batch_run

elapsed_times = {}

## Run 8 scripts to compare:-
## - CSV vs Parquet
## - Rust, C++, C# & Go dataframe libraries
## - Peakrs(Rust), Peakcs(C#) & Peakgo (Golang)
##     are dataframe library
## - Run Rust dataframe library by Rust and Python
## - Data volumn: 1M, 10M, 100M, 1,000M, run each for 5 times
## - Select best 2 to run 10,000M (10 Billion Rows)
## - Query command: Filter, Inner_Join & Group_By

## Procedure
## - Filter master record
## - Filter fact table
## - Join 2 table by inner join method
## - Output detail report
## - Multiply Unit_Price by Quantity as Amount
## - Group by and output summary report

scripts = ["python polar-parquet.py", 
           "python duckdb-parquet.py", 
           "python polar-csv.py",           
           "python duckdb-csv.py",            
           "python peakpy.py", 
           "peakrs",
           "peakcs", 
           "peakgo"]         

data_file_name, _ = os.path.splitext(data_file)
result = f"Outbox/InnerJoin_{data_file_name}_benchmark.csv"

elapsed_times_scripts = {script: [] for script in scripts}

# Calculate total_runs based on batch_run and the number of scripts
total_runs = batch_run * len(scripts)

# Generate the batches list based on batch_run
batches = [f'Batch{i+1}' for i in range(batch_run)]

outbox_dir = 'Outbox'

# Check if Outbox folder has any file or folder not matching the current case's Benchmark pattern or BatchX
for name in os.listdir(outbox_dir):
    if (os.path.isfile(os.path.join(outbox_dir, name)) or os.path.isdir(os.path.join(outbox_dir, name))) and not re.match(f'Benchmark_.*_\d+_\d{{8}}_\d{{6}}', name) and not name.startswith('Batch'):
        print("The benchmark can start only if the Outbox folder contains only Benchmark folders that match the current case's naming convention or Batch folders.")
        exit(1)

for i in range(total_runs):
    script = scripts[i % len(scripts)]    
    print(f"\n*** Start Process: {script} Test Run: {i+1} of {total_runs} ***")
    
    # Check if the script contains the word "parquet"
    if 'parquet' in script:
        data_file_ext = data_file + '.parquet'
    else:
        data_file_ext = data_file + '.csv'
    
    # Count the number of files in the Outbox before running the script
    before_files_count = len([name for name in os.listdir(outbox_dir) if os.path.isfile(os.path.join(outbox_dir, name))])
    
    start_time = datetime.now()
    if script != "do oldpeaks_innerjoin":
         subprocess.run(script.split() + [data_file_ext])
    else:
         data_file2 = "File=" + data_file_ext         
         subprocess.run(script.split() + [data_file2])

    # Count the number of files in the Outbox after running the script
    after_files_count = len([name for name in os.listdir(outbox_dir) if os.path.isfile(os.path.join(outbox_dir, name))])

    # If no new file is found, mark the elapsed time as "Fail"
    if after_files_count <= before_files_count:
        elapsed_seconds = 'Fail'
        print(f"\n{script} failed to output a file.")
    else:
        elapsed = datetime.now() - start_time
        elapsed_seconds = round(elapsed.total_seconds(), 2)
        print(f"\n*** {script} Duration (in second): {elapsed_seconds} ***")
    
    elapsed_times_scripts[script].append(elapsed_seconds)

    if (i+1)%len(scripts) == 0:
        # Move the files to the batch folder after each complete run
        batch_dir = os.path.join(outbox_dir, batches[i//len(scripts)])
        if not os.path.exists(batch_dir):
            os.makedirs(batch_dir)
        for file_name in os.listdir(outbox_dir):
            if (file_name.endswith('.csv') or file_name.endswith('.parquet')) and file_name != os.path.basename(result):
                os.rename(os.path.join(outbox_dir, file_name), os.path.join(batch_dir, file_name))

        for _ in range(3):
            print()

        print(f"\n*** See Benchmark Result: {data_file} ***")
        print()

        ## Write benchmark records to disk
        with open(result, 'w', newline='') as f:
            writer = csv.writer(f)   
            writer.writerow(["Test Case"] + [f"Run {i+1}" for i in range(len(list(elapsed_times_scripts.values())[0]))] + ["Average"])    
            for script, times in elapsed_times_scripts.items():
                # Calculate the average time for all runs and add it as a row
                avg_time = round(sum(t for t in times if isinstance(t, float))/len(times), 2) if all(isinstance(t, float) for t in times) else 'Fail'
                writer.writerow([script] + times + [avg_time])

        ## Display benchmark results to screen

        with open(result, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
            widths = [max(map(len, col)) for col in zip(*rows)]
            for row in rows:
                # Add underline to the column names
                if rows.index(row) == 0:
                    print("  ".join((f'\033[4m{val.ljust(width)}\033[0m' for val, width in zip(row, widths))))
                else:
                    print("  ".join((val.ljust(width) for val, width in zip(row, widths))))

        print()

# Create a new benchmark folder for the current run
now = datetime.now()
benchmark_dir = os.path.join(outbox_dir, f"Benchmark_{data_file}_{batch_run}_{now.strftime('%Y%m%d_%H%M%S')}")
if not os.path.exists(benchmark_dir):
    os.makedirs(benchmark_dir)

# Move only the new files and batch folders generated in the current run to the new benchmark folder
for file_name in os.listdir(outbox_dir):
    if (file_name.endswith('.csv') or file_name.endswith('.parquet') or file_name.startswith('Batch')) and not file_name.startswith('Benchmark_'):
        os.rename(os.path.join(outbox_dir, file_name), os.path.join(benchmark_dir, file_name))
