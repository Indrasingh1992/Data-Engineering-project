import csv
import random
from datetime import datetime, timedelta
from multiprocessing import Process, cpu_count, Manager
import os
import math

# Define constants
OUTPUT_FILE = "large_tables1.csv"
TEMP_FOLDER = "temp_files"
ROW_SIZE = 8060  # Estimated row size, you may adjust this based on actual row content
TOTAL_SIZE_GB = 20  # Target file size in GB

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

def merge_chunk(temp_files_chunk, output_file, return_dict, process_id):
    """Merges a chunk of temporary files into a single CSV file."""
    with open(output_file, mode='a', newline='') as outfile:
        writer = csv.writer(outfile)
        header_written = False

        for temp_file in temp_files_chunk:
            with open(temp_file, mode='r') as infile:
                reader = csv.reader(infile)
                header = next(reader)  # Skip the header of each file
                if not header_written:
                    writer.writerow(["ID", "StaticText", "VariableText", "LargeNumber", "CreatedDate", "Status"])  # Write header once
                    header_written = True
                writer.writerows(reader)

    return_dict[process_id] = f"Process {process_id} completed"

def parallel_generate_csv(total_rows, processes, temp_folder):
    """Splits work among processes and runs them in parallel to generate CSV chunks."""
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

def parallel_merge_csv(temp_folder, output_file, num_processes):
    """Merges all temporary files using parallel processing."""
    temp_files = sorted(os.listdir(temp_folder))
    chunk_size = len(temp_files) // num_processes
    processes_list = []
    manager = Manager()
    return_dict = manager.dict()

    for i in range(num_processes):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < num_processes - 1 else len(temp_files)
        temp_files_chunk = [os.path.join(temp_folder, temp_files[j]) for j in range(start_index, end_index)]
        
        process = Process(target=merge_chunk, args=(temp_files_chunk, output_file, return_dict, i))
        processes_list.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes_list:
        process.join()

    # Print the status of each process
    for process_id, status in return_dict.items():
        print(status)

# Main execution
if __name__ == "__main__":
    # Calculate the number of rows to generate to reach 20GB (adjust based on row size)
    total_size_in_gb = TOTAL_SIZE_GB
    total_rows = math.ceil(total_size_in_gb * 1024 * 1024 * 1024 / ROW_SIZE)

    NUM_PROCESSES = cpu_count()  # Number of processes to run
    print(f"Using {NUM_PROCESSES} processes...")

    # Step 1: Generate CSV files in parallel
    print("Generating CSV files in parallel...")
    parallel_generate_csv(total_rows, NUM_PROCESSES, TEMP_FOLDER)

    # Step 2: Merge temporary files into the final CSV using parallel processing
    print("Merging temporary files using parallel processing...")
    parallel_merge_csv(TEMP_FOLDER, OUTPUT_FILE, NUM_PROCESSES)

    # Step 3: Clean up temporary files
    for temp_file in os.listdir(TEMP_FOLDER):
        os.remove(os.path.join(TEMP_FOLDER, temp_file))
    os.rmdir(TEMP_FOLDER)

    print(f"CSV generation and merging completed: {OUTPUT_FILE}")
