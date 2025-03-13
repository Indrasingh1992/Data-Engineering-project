import csv
import random
from datetime import datetime, timedelta

# Generate 50 million rows
rows = 50184528
output_file = "large_table.csv"

with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["ID", "StaticText", "VariableText", "LargeNumber", "CreatedDate", "Status"])
    
    for i in range(1, rows + 1):
        static_text = "A" * 200
        variable_text = "B" * 200 + str(i)
        large_number = round(random.uniform(0, 1000000), 2)
        created_date = (datetime(2000, 1, 1) + timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')
        status = 'Y' if i % 2 == 0 else 'N'
        writer.writerow([i, static_text, variable_text, large_number, created_date, status])
