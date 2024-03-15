import sys

if len(sys.argv) != 2:
	print "Usage: python2 parse_stats.py <stat_file_name>"
	exit()

fname = sys.argv[1]
f = open(fname, 'r')
csv_to_arrow = 0
arrow_to_velox = 0
decomposition = 0
velox_to_arrow = 0
arrow_to_csv = 0
decomposition_total = 0
for line in f:
	if (line[0] == "*"):
		# Reset
		csv_to_arrow = 0
		arrow_to_velox = 0
		decomposition = 0
		velox_to_arrow = 0
		arrow_to_csv = 0
		decomposition_total = 0
		print line
		continue
	if "Time for loading CSV into Arrow" in line:
		t = line.split(":")[1]
		t = t.split("ns")[0]
		t = int(t)
		csv_to_arrow += t
	if "Time for porting from Arrow to Velox" in line:
		t = line.split(":")[1]
		t = t.split("ns")[0]
		t = int(t)
		arrow_to_velox += t
	if "Time for decomposition" in line:
		t = line.split(":")[1]
		t = t.split("ns")[0]
		t = int(t)
		decomposition += t
	if "Time to port velox to arrow" in line:
		t = line.split(":")[1]
		t = t.split("ns")[0]
		t = int(t)
		velox_to_arrow += t
	if "Time to store to CSV" in line:
		t = line.split(":")[1]
		t = t.split("ns")[0]
		t = int(t)
		arrow_to_csv += t
	if "Decomposed CSV SIZE total" in line:
		print "Time for loading CSV into Arrow: ", csv_to_arrow, " ns"
		print "Time for porting from Arrow to Velox: ", arrow_to_velox, " ns"
		print "Time for decomposition: ", decomposition, " ns"
		print "Time to port velox to arrow: ", velox_to_arrow, " ns"
		print "Time to store to CSV: ", arrow_to_csv, " ns"
		print line
		continue
	if "Decomposition time" in line:
		print line
		continue
	if "ZIP SIZE" in line:
		print line
		continue
	if "Zip creation time" in line:
		print line
		continue

	
f.close()