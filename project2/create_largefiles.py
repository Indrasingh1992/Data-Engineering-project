import csv
import random
from datetime import datetime, timedelta
from multiprocessing import Process, cpu_count
import os

# Total rows and output file name
TOTAL_ROWS = 50184528
OUTPUT_FILE = "large_tables.csv"
TEMP_FOLDER = "temp_files"

# Create temp folder to store temporary files
os.makedirs(TEMP_FOLDER, exist_ok=True)

def generate_csv(start, end, filename):
    """Generates a chunk of the CSV file."""
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        for i in range(start, end + 1):
            static_text = "A" * 200
            variable_text = "B" * 200 + str(i)
            large_number = round(random.uniform(0, 1000000), 2)
            created_date = (datetime(2000, 1, 1) + timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')
            status = 'Y' if i % 2 == 0 else 'N'
            writer.writerow([i, static_text, variable_text, large_number, created_date, status])

def merge_csv_files(output_file, temp_folder):
    """Merges all temporary files into a single CSV file."""
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.writer(outfile)
        # Write header row
        writer.writerow(["ID", "StaticText", "VariableText", "LargeNumber", "CreatedDate", "Status"])
        # Read and append rows from each temp file
        for temp_file in sorted(os.listdir(temp_folder)):
            with open(os.path.join(temp_folder, temp_file), mode='r') as infile:
                reader = csv.reader(infile)
                writer.writerows(reader)

def parallel_generate_csv(total_rows, processes, temp_folder):
    """Splits work among processes and runs them in parallel."""
    chunk_size = total_rows // processes
    processes_list = []

    for i in range(processes):
        start = i * chunk_size + 1
        end = (i + 1) * chunk_size if i < processes - 1 else total_rows
        temp_file = os.path.join(temp_folder, f"temp_{i + 1}.csv")
        process = Process(target=generate_csv, args=(start, end, temp_file))
        processes_list.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes_list:
        process.join()

# Main execution
if __name__ == "__main__":
    NUM_PROCESSES = cpu_count()  # Number of processes to run
    print(f"Using {NUM_PROCESSES} processes...")

    # Step 1: Generate CSV files in parallel
    parallel_generate_csv(TOTAL_ROWS, NUM_PROCESSES, TEMP_FOLDER)

    # Step 2: Merge temporary files into the final CSV
    print("Merging temporary files...")
    merge_csv_files(OUTPUT_FILE, TEMP_FOLDER)

    # Step 3: Clean up temporary files
    for temp_file in os.listdir(TEMP_FOLDER):
        os.remove(os.path.join(TEMP_FOLDER, temp_file))
    os.rmdir(TEMP_FOLDER)

    print(f"CSV generation completed: {OUTPUT_FILE}")
