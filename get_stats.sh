#!/bin/bash

g++ scripts/size_in_mb.cpp -o mb.out
cd velox
make release NUM_THREADS=8 MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=2 EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_ARROW=ON"
cd ..

# Cleanup
rm -r split_dataset
rm -r data
rm -r dim*
rm -r fact.csv
rm -r data.zip
rm -r split_dataset.zip

dataset_folder="data/"
dest_folder="split_dataset/"

# Hack for find to work when there are spaces in filenames
IFS=$'\n'; set -f
for zip in $(find datasets/ -name *.zip)
do
	echo ""
	orig_size=0
	split_size=0
	splitting_time=0
	echo "****** $zip ******"
	unzip "$zip" -d data

    for csv in $(find $dataset_folder -name "*.csv")
    do
        echo "Splitting file ${csv}"
        csv_size=$(find $csv -printf "%s")
        orig_size=$(expr $csv_size + $orig_size)
        foldername=$(python scripts/gen_folder_name.py ${csv} ${dataset_folder})
        foldername="$dest_folder/$foldername"
        mkdir -p $foldername

        # Generate split csv
        start=`date +%s%N`
        ./velox/_build/release/velox/examples/split_csv $csv

        split_generated=0
        for f in $(find . -maxdepth 1 -name "fact.csv")
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
        for f in $(find . -maxdepth 1 -name "dim*")
        do
            python3 scripts/remove_quotes.py ${f}
            n=$(expr $n + 1)
            csv_size=$(find $f -printf "%s")
            split_size=$(expr $csv_size + $split_size)
            mv $f $foldername
        done
        python3 scripts/remove_quotes.py fact.csv
        csv_size=$(find . -maxdepth 1 -name "fact.csv" -printf "%s")
        split_size=$(expr $csv_size + $split_size)
        mv fact.csv $foldername

        end=`date +%s%N`

        splitting_time=$(expr $end - $start + $splitting_time)

        echo "Generated $n dimension tables"
    done

    echo ""

    echo -n "Size of original CSV dataset: "
    ./mb.out "$orig_size"
    echo -n "Size of split CSV dataset: "
    ./mb.out "$split_size"
    echo -n "Compressed by "
    ratio=$(python3 scripts/calculate_ratio.py $orig_size $split_size)
    echo $ratio 'x'
    echo -n "Splitting time: " $splitting_time "ns"

	# Calculate the time taken and size of the zip
	echo ""
	echo ""
	start=`date +%s%N`
	zip -r data.zip data/
	end=`date +%s%N`
	zip_size=$(find . -maxdepth 1 -name data.zip -printf "%s")
	echo -n "Original ZIP size: "
	./mb.out "$zip_size"
	echo "Original zip creation time: `expr $end - $start` nanoseconds."
	echo ""

	# Calculate the time taken and size of the zip for split_dataset
	echo ""
	start=`date +%s%N`
	zip -r split_dataset.zip split_dataset/
	end=`date +%s%N`
	split_zip_size=$(find . -maxdepth 1 -name split_dataset.zip -printf "%s")
	echo -n "Split ZIP size: "
	./mb.out "$split_zip_size"
	echo "Split zip creation time: `expr $end - $start` nanoseconds."
	echo ""

	echo "PyArrow (python3) CSV loading bechmark"
	echo "Load original dataset into PyArrow (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/pyarrow_benchmark.py data/
		cat output.txt | grep "Maximum"
	done

	echo "Load split dataset into PyArrow (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/pyarrow_benchmark.py split_dataset/
		cat output.txt | grep "Maximum"
	done
	echo ""

	echo "pandas (python3) CSV loading bechmark"
	echo "Load original dataset into pandas (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/pandas_loading_benchmark.py data/
		cat output.txt | grep "Maximum"
	done

	echo "Load split dataset into pandas (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/pandas_loading_benchmark.py split_dataset/
		cat output.txt | grep "Maximum"
	done
	echo ""

	echo "DuckDB (python3) CSV loading bechmark"
	echo "Load original dataset into DuckDB (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/duckdb_loading_benchmark.py data/
		cat output.txt | grep "Maximum"
	done

	echo "Load split dataset into DuckDB (repeated five times)"
	for i in {1..5}
	do
		echo $i
		/usr/bin/time -o output.txt -v python3 scripts/duckdb_loading_benchmark.py split_dataset/
		cat output.txt | grep "Maximum"
	done
	echo ""
 
	rm -r data
	rm -r split_dataset
	rm data.zip
	rm split_dataset.zip
	rm output.txt
	echo ""
	echo ""
done
unset IFS; set +f
rm mb.out