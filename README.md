## Overview

This is a module for prototyping the concepts of *splitting*. The goal of splitting is to remove redundancy from the data when performing data analysis on tabular datasets. Tabular datasets for data analysis are typically distributed in open formats such as CSV, JSON, Parquet, etc., with CSV being the most popular format (see [Kaggle](www.kaggle.com/)). This module consists of two components:

- *Generating Split CSV datasets*: We implement module for generating split CSV datasets in [Velox](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/) which takes as input a regular CSV dataset and splits the individual files to generate a split dataset. The module implements the SplitGen algorithm to generate a split schema based on statistics collected from the data, and is fully automated (see `velox/velox/examples/SplitCSV.cpp`).

- *Split dataframes in Ibis*: We prototype split dataframes in Ibis, which internally operate on split datasets (see `ibis/ibis/backends/duckdb/__init__.py`).

## Generating Split CSV datasets

The code for generating Split CSV is in the file `velox/velox/examples/SplitCSV.cpp`. Below are the instructions for building and running this module to generate split CSV datasets.

### Requirements and Building

Velox requirements are:

* C++17 , thus minimum supported compiler is GCC 5.0 and Clang 5.0.
* CPU to support instruction sets:
  * bmi
  * bmi2
  * f16c 

This module has been tested on an x86_64 machine with Ubuntu 20.04 (and later). To set up Velox:

```
sudo apt install virtualenv
virtualenv -p /usr/bin/python3.10 VENV
source VENV/bin/activate
pip install pandas==2.0.3
cd velox
sudo ./scripts/setup-ubuntu.sh
make release NUM_THREADS=8 MAX_HIGH_MEM_JOBS=4 MAX_LINK_JOBS=2 EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_ARROW=ON"
cd ..
```

### Running

* First step is to download a CSV dataset. There are a number of datasets available on Kaggle, some of which have been listed in `datasets/links.txt` in increasing order of size. We recommend downloading the FIFA dataset to get started.

```unzip archive.zip -d fifa```

* To generate split CSV for a dataset of multiple CSV files, run

```./gen_split_csv_dataset.sh dataset_folder_name [dest_folder]```

Typically a dataset can contain multiple CSV files. For each CSV file in the dataset, the script generates a star schema and stores it in a subfolder (with the same name as the CSV file) in the dest_folder.

To generate split CSV for a single file, run:

```./gen_split_csv.sh csv_filename dest_folder```

The above command generates a star schema and stores it in the destination folder.

## Split Dataframes in Ibis

To prototype split dataframes in Ibis, we implemented a query rewriting layer for the DuckDB backend (file `ibis/ibis/backends/duckdb/__init__.py`). The following steps set up the Ibis split dataframe prototype.

### Requirements and Building

```
cat requirements.txt | xargs -n 1 pip install
git submodule init
git submodule update
cd ibis
sudo apt install libpq-dev
pip install 'poetry==1.8.2'
pip install -r requirements-dev.txt
pip install -e .
cd ..
```

### Ibis notebooks

Sample notebooks operating on split dataframes are present under the folder `notebooks` for reference. To run these notebooks, first download the corresponding dataset and perform splitting using the instructions above. Then move the original and split dataset to the right location before running the notebooks (refer to the README in each of the `notebooks/*` folders).

For instance, download the US Accidents dataset from this [link](https://drive.google.com/file/d/1-uePluCnhkyQPP392_Yt95dQyKHfd5lJ/view?usp=sharing). Then run:

```
unzip us-accidents.zip -d us-accidents
./gen_split_csv_dataset.sh us-accidents US_Accidents_Dec21_updated_split
mv us-accidents/US_Accidents_Dec21_updated.csv notebooks/us-accidents && rmdir us-accidents
mv US_Accidents_Dec21_updated_split notebooks/us-accidents
cd notebooks/us-accidents
```

To run a notebook,

```
/usr/bin/time ipython3 Notebook\ 1.py
```

One can toggle between regular and split dataframes by changing the input source. See cell [2] in `notebooks/us-accidents/Notebook\ 1.py` for an example.

The `/usr/bin/time` library provides helpful statistics such as the the running time and the maximum resident set size (RSS) of the process while running the notebook.
