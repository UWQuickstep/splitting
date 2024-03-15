import sys
import os
import time
import duckdb

MB = 1000000.0

if len(sys.argv) != 2:
	print("USAGE: python3 duckdb_loading_benchmark.py <foldername>")
	exit()

foldername = sys.argv[1]
duckdb_tables = []

# Load all the CSV files in the folder
start = time.time_ns()
for root, dirs, files in os.walk(foldername):
	for file in files:
		if file.endswith(".csv"):
			# print(os.path.join(root, file))
			filename = os.path.join(root, file)
			try:
				table = duckdb.sql("SELECT * FROM read_csv_auto('{}')".format(filename))
				table.execute()
			except:
				try:
					# HACK: load the whole file to fix type interpretation problem
					table = duckdb.sql("SELECT * FROM read_csv_auto('{}', sample_size=-1)".format(filename))
					table.execute()
				except:
					print("Could not load file", filename)
					continue
			duckdb_tables.append(table)
end = time.time_ns()
print("Time elapsed loading from", foldername, "for DuckDB is", end - start, "nanoseconds")