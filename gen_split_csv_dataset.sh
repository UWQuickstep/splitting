#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Usage: ./gen_split_csv_dataset dataset_folder [dest_folder]"
    echo ""
    echo "Split CSV files in the dataset and store the output in optionally specified dest_folder"
    exit 1
fi

# Build
g++ scripts/size_in_mb.cpp -o mb.out
cd velox
make release NUM_THREADS=8 MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=2 EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_ARROW=ON"
cd ..

dataset_folder=$1
if [ -z $2 ]; then
    dest_folder="split_dataset/"
else
    dest_folder=$2
fi
mkdir -p $dest_folder
echo ""

orig_size=0
split_size=0
for csv in $(find $dataset_folder -name "*.csv")
do
    echo "Splitting file ${csv}"
    csv_size=$(find $csv -printf "%s")
    orig_size=$(expr $csv_size + $orig_size)
    foldername=$(python scripts/gen_folder_name.py ${csv} ${dataset_folder})
    foldername="$dest_folder/$foldername"
    mkdir -p $foldername

    # Generate split csv
    ./velox/_build/release/velox/examples/split_csv $csv

    split_generated=0
    for f in $(find ./* -maxdepth 1 -name "fact.csv")
    do
        split_generated=1
    done
    if [ $split_generated -eq 0 ]
    then
        cp ${csv} ${foldername}/fact.csv
        csv_size=$(find ${csv} -printf "%s")
        split_size=$(expr $csv_size + $split_size)
        continue
    fi

    # Load into pandas and overwrite to remove quotes
    n=0
    for f in $(find ./* -maxdepth 1 -name "dim*")
    do
        python3 scripts/remove_quotes.py ${f}
        n=$(expr $n + 1)
        csv_size=$(find $f -printf "%s")
        split_size=$(expr $csv_size + $split_size)
    done
    python3 scripts/remove_quotes.py fact.csv
    csv_size=$(find "fact.csv" -printf "%s")
    split_size=$(expr $csv_size + $split_size)

    mv dim* $foldername
    mv fact.csv $foldername

    echo "Generated $n dimension tables"
done

echo ""
echo "Split CSV dataset stored in folder ${dest_folder}"

echo -n "Size of original CSV dataset: "
./mb.out "$orig_size"
echo -n "Size of split CSV dataset: "
./mb.out "$split_size"
echo -n "Compressed by "
ratio=$(python3 scripts/calculate_ratio.py $orig_size $split_size)
echo $ratio 'x'

# Cleanup
rm mb.out
