import sys
import os
import time
import pandas as pd

MB = 1000000.0

if len(sys.argv) != 2:
	print("USAGE: python3 pandas_loading_benchmark.py <foldername>")
	exit()

foldername = sys.argv[1]
pandas_dfs = []

# Load all the CSV files in the folder
start = time.time_ns()
for root, dirs, files in os.walk(foldername):
	for file in files:
		if file.endswith(".csv"):
			# print(os.path.join(root, file))
			filename = os.path.join(root, file)
			try:
				df = pd.read_csv(filename)
			except:
				try:
					# HACK
					df = pd.read_csv(filename, encoding='latin-1')
				except:
					print("Could not load file", filename)
					continue
			pandas_dfs.append(df)
end = time.time_ns()
print("Time elapsed loading from", foldername, "for pandas is", end - start, "nanoseconds")
