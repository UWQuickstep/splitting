import sys
import os
import time
from pyarrow import csv

MB = 1000000.0

if len(sys.argv) != 2:
	print("USAGE: python3 pyarrow_benchmark.py <foldername>")
	exit()

foldername = sys.argv[1]
arrow_tables = []
total_size = 0

# Load all the CSV files in the folder
start = time.time_ns()
for root, dirs, files in os.walk(foldername):
	for file in files:
		if file.endswith(".csv"):
			# print(os.path.join(root, file))
			filename = os.path.join(root, file)
			table = csv.read_csv(filename)
			arrow_tables.append(table)
end = time.time_ns()

print("Time elapsed loading from", foldername, "for pyarrow is", end - start, "nanoseconds")