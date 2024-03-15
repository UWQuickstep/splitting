import sys

filename = sys.argv[1]
dataset_folder = sys.argv[2]
if dataset_folder[-1] != '/':
	dataset_folder += '/'
# foldername = filename.split("/")[-1]
# foldername = foldername.split(".csv")[0]
foldername = filename.split(dataset_folder)[-1]
foldername = foldername.split(".csv")[0]
print(foldername)