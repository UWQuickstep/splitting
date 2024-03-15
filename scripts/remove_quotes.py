import pandas as pd
import sys

if len(sys.argv) != 2:
	print("USAGE: python3 remove_quotes.py filename.csv")
	exit(1)

filename = sys.argv[1]
df = pd.read_csv(filename, dtype=object)
# Don't write the index column
df.to_csv(filename, index=False)