import os
import subprocess
from datetime import datetime
import csv
import argparse

# Handle command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('data_file', nargs='?', default='1M_Fact.csv')
parser.add_argument('batch_run', nargs='?', default=3, type=int)
args = parser.parse_args()

data_file = args.data_file
batch_run = args.batch_run

elapsed_times = {}
scripts = ["python polars_streaming_innerjoin.py", "python polars_sinkcsv_innerjoin.py", "polars_innerjoin.exe", "python peakrs_innerjoin.py", "peakrs_innerjoin.exe", "gopeaks_innerjoin.exe", "gopeaks_turbo_innerjoin.exe", "do oldpeaks_innerjoin"]
data_file_name, _ = os.path.splitext(data_file)
result = f"Outbox/InnerJoin_{data_file_name}_benchmark.csv"

elapsed_times_scripts = {script: [] for script in scripts}

# Calculate total_runs based on batch_run and the number of scripts
total_runs = batch_run * len(scripts)

# Generate the batches list based on batch_run
batches = [f'Batch{i+1}' for i in range(batch_run)]

outbox_dir = 'Outbox'

for i in range(total_runs):
    script = scripts[i % len(scripts)]    
    print(f"\n********* Start Process: {script} Test Run: {i+1} of {total_runs} *********")
    
    # Count the number of files in the Outbox before running the script
    before_files_count = len([name for name in os.listdir(outbox_dir) if os.path.isfile(os.path.join(outbox_dir, name))])
    
    start_time = datetime.now()
    if script != "do oldpeaks_innerjoin":
         subprocess.run(script.split() + [data_file])
    else:
         data_file2 = "File=" + data_file         
         subprocess.run(script.split() + [data_file2])

    # Count the number of files in the Outbox after running the script
    after_files_count = len([name for name in os.listdir(outbox_dir) if os.path.isfile(os.path.join(outbox_dir, name))])

    # If no new file is found, mark the elapsed time as "Fail"
    if after_files_count <= before_files_count:
        elapsed_seconds = 'Fail'
        print(f"\n{script} failed to output a file.")
    else:
        elapsed = datetime.now() - start_time
        elapsed_seconds = round(elapsed.total_seconds(), 1)
        print(f"\n{script} Duration (in second): {elapsed_seconds}")
    
    elapsed_times_scripts[script].append(elapsed_seconds)

    if (i+1)%len(scripts) == 0:
        # Move the files to the batch folder after each complete run
        batch_dir = os.path.join(outbox_dir, batches[i//len(scripts)])
        if not os.path.exists(batch_dir):
            os.makedirs(batch_dir)
        for file_name in os.listdir(outbox_dir):
            if file_name.endswith('.csv') and file_name != os.path.basename(result):
                os.rename(os.path.join(outbox_dir, file_name), os.path.join(batch_dir, file_name))

        for _ in range(3):
            print()

        print(f"\n********* See Benchmark Result: {data_file} *********")
        print()

        ## Write benchmark records to disk
        with open(result, 'w', newline='') as f:
            writer = csv.writer(f)   
            writer.writerow(["Test Case"] + [f"Run {i+1}" for i in range(len(list(elapsed_times_scripts.values())[0]))] + ["Average"])    
            for script, times in elapsed_times_scripts.items():
                # Calculate the average time for all runs and add it as a row
                avg_time = round(sum(times)/len(times), 1) if isinstance(times[0], float) else 'Fail'
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
