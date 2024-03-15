#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Usage: ./gen_split_csv csv_file_name [dest_folder]"
    echo ""
    echo "Generate split CSV for file and store the output in optionally specified dest_folder"
    exit 1
fi

# Build
g++ scripts/size_in_mb.cpp -o mb.out
cd velox
make release NUM_THREADS=8 MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=2 EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_ARROW=ON"
cd ..

fname=$1
if [ -z $2 ]; then
    dest_folder=$(python scripts/gen_folder_name.py ${fname})
else
    dest_folder=$2
fi
mkdir -p $dest_folder
echo "Destination folder", $dest_folder

echo ""
echo "Splitting file" $fname

# Generate split csv
./velox/_build/release/velox/examples/split_csv $fname

split_generated=0
for f in $(find ./* -maxdepth 1 -name "fact.csv")
do
    split_generated=1
done
if [ $split_generated -eq 0 ]
then
    cp ${fname} ${dest_folder}/fact.csv
    exit 0
fi

# Load into pandas and overwrite to remove quotes
for f in $(find . -name "dim*")
do
	python3 scripts/remove_quotes.py ${f}
done
python3 scripts/remove_quotes.py fact.csv

mv dim* $dest_folder
mv fact.csv $dest_folder

echo ""
echo "Split CSV stored in folder" $dest_folder

# Calculate some stats
n=0
orig_size=$(find ""$fname"" -printf "%s")
split_size=0
for dim in $(find $dest_folder -name "dim*")
do
    n=$(expr $n + 1)
    dim_size=$(find ""$dim"" -printf "%s")
    split_size=$(expr $split_size + $dim_size)
done
fact_size=$(find "$dest_folder/fact.csv" -printf "%s")
split_size=$(expr $split_size + $fact_size)
echo "Generated $n dimension tables"
echo -n "Size of original CSV: "
./mb.out "$orig_size"
echo -n "Size of split CSV: "
./mb.out "$split_size"
echo -n "Compressed by "
ratio=$(python3 scripts/calculate_ratio.py $orig_size $split_size)
echo $ratio 'x'

# Cleanup
rm mb.out
