#!/usr/bin/env python
# coding: utf-8

# This notebook is re-implementing the data manipulation portions of the most voted notebook https://www.kaggle.com/code/jingzongwang/usa-car-accidents-severity-prediction#2-HANDLING-MISSING-DATA on Kaggle for the US-Accidents dataset. The original notebook is written using the `pandas` library, here we are re-writing only the data manipulation operations (omitting plotting, training, etc.) in `ibis` with **DuckDB** backend.
# 
# To switch between the default and split CSV format, use `init_ddb_from_csv` and `init_ddb_from_split_csv` respectively.
# 
# The code for printing has been commented out. Uncomment to debug.
# 
# Note that the CSV file used in the original notebook appears to be slightly different from the presently downloaded CSV file, so the output is slightly different. I have cross-checked the output by running the code from the original notebook on this dataset.

# In[1]:

import time


def init_ddb_from_csv(db_filename, tablename, csv_filename, **kwargs):
    """
    Load from the csv file into a DuckDB database.
    
    db_filename: Name of the database
    tablename: Table to load to
    csv_filename: CSV file to load from
    **kwargs: Options for DuckDB's read_csv function, see https://duckdb.org/docs/data/csv/overview
    """
    import duckdb
    duckdb_con = duckdb.connect(db_filename)
    read_csv_args_list = ["'{}'".format(csv_filename)]
    schema = {tablename : {
        "fact" : tablename + "_fact",
        "dimension_tables" : {},
        "col_to_table_map" : {}
    }}
    for key, value in kwargs.items():
        read_csv_args_list.append("{0} = {1}".format(key, value))
    read_csv_args = ','.join(read_csv_args_list)
    sql_stmt = "CREATE TABLE {} AS SELECT * FROM read_csv({}, AUTO_DETECT=TRUE)".format(tablename, read_csv_args)
    print(sql_stmt)
    duckdb_con.sql(sql_stmt)
    table = duckdb_con.table(tablename)
    for col in table.columns:
        schema[tablename]["col_to_table_map"][col] = schema[tablename]["fact"]
    duckdb_con.close()
    return schema

def init_ddb_from_split_csv(db_filename, tablename, split_csv_foldername, **kwargs):
    """
    Load the split csv file into a DuckDB database and expose a view with tablename

    db_filename: Name of the database
    tablename: View to expose giving the impression of a table
    csv_filename: Folder containing the split CSV files
    **kwargs: Options for DuckDB's read_csv function, see https://duckdb.org/docs/data/csv/overview
    """
    import duckdb
    import os
    duckdb_con = duckdb.connect(db_filename)
    schema = {tablename : {
        "fact" : tablename + "_fact",
        "dimension_tables" : {},
        "col_to_table_map" : {}
    }}
    # read_csv_args_list = ["'{}'".format(csv_filename)]
    # for key, value in kwargs.items():
    #     read_csv_args_list.append("{0} = {1}".format(key, value))
    # read_csv_args = ','.join(read_csv_args_list)
    num_dims = 0
    cols = []
    sub_tablenames = []
    for root, dirs, files in os.walk(split_csv_foldername):
        for file in files:
            sub_tablename = tablename + "_" + file.split(".csv")[0]
            sub_tablenames.append(sub_tablename)
            if 'dim' in file:
                num_dims += 1
                # Assuming that dimension tables are named dimx.csv
                dim_no = sub_tablename.split("dim")[1]
                schema[tablename]["dimension_tables"][sub_tablename] = 'p' + dim_no
            full_filename = root + '/' + file
            sql_stmt = "CREATE TABLE {} AS SELECT * FROM read_csv('{}', AUTO_DETECT=TRUE)".format(sub_tablename, full_filename)
            print(sql_stmt)
            duckdb_con.sql(sql_stmt)
            table = duckdb_con.table(sub_tablename)
            for col in table.columns:
                # HACK: not fool proof, the CSV could contain a column starting with letter 'p'
                if col[0] == 'p':
                    continue
                cols.append('"' + col + '"')
                schema[tablename]['col_to_table_map'][col] = sub_tablename

    # Now create a view corresponding to a single original csv file
    join_clauses = []
    for i in range(num_dims):
        join_clause = "{}_fact.p{} = {}_dim{}.p{}".format(tablename, i, tablename, i, i)
        join_clauses.append(join_clause)

    sql_stmt = "CREATE VIEW {} AS SELECT ".format(tablename) + ",".join(cols) + \
        " FROM " + ",".join(sub_tablenames) + " WHERE " + (" AND ").join(join_clauses)
    print(sql_stmt)
    duckdb_con.sql(sql_stmt)
    duckdb_con.close()
    return schema


# In[2]:

start = time.time()

dbname = "us_accidents.db"
tablename = "accidents"

# # Default -- loading directly from CSV
# csv_filename = "US_Accidents_Dec21_updated.csv"
# schema = init_ddb_from_csv(dbname, tablename, csv_filename)

# # Split -- loading from split CSV
split_csv = "US_Accidents_Dec21_updated_split"
schema = init_ddb_from_split_csv(dbname, tablename, split_csv)

end = time.time()
print("Cell [2] time:", end-start)
print()
print()


# In[3]:


schema


# In[4]:


start = time.time()

import ibis
import pandas as pd

# Since we want all expressions to run, even if the output is not used
# By default, expressions are lazily evaluated
# The function call to_pandas() explicitly evaluates an expression
# We want to avoid invoking to_pandas() all the time
ibis.options.interactive = True

con = ibis.duckdb.connect(dbname)
con.register_schema(schema)

end = time.time()
print("Cell [4] time:", end-start)
print()
print()

# In[5]:


start = time.time()

table = con.table(tablename)
print(table)

end = time.time()
print("Cell [5] time:", end-start)
print()
print()


# In[6]:


start = time.time()

table = table.mutate(Year = table.Start_Time.year())
table = table.mutate(Month = table.Start_Time.month())

end = time.time()
print("Cell [6] time:", end-start)
print()
print()


# In[7]:

start = time.time()

table = table.mutate(Day = table.Start_Time.day())
table = table.mutate(Hour = table.Start_Time.hour())

end = time.time()
print("Cell [7] time:", end-start)
print()
print()


# In[8]:


start = time.time()

table = table.mutate(Weekday = table.Start_Time.day_of_week.index())

end = time.time()
print("Cell [8] time:", end-start)
print()
print()


# In[9]:


start = time.time()

missing_df = {"column_name": table.columns}
missing_counts = []
for col in table.columns:
    # to_pandas converts ibis.IntegerScalar type to np.int64
    missing_count = table[col].isnull().sum().to_pandas()
    missing_counts.append(missing_count)
missing_df["missing_count"] = missing_counts
missing_df = pd.DataFrame(missing_df)

# Skipping plotting

missing_table = ibis.memtable(missing_df)
count = table.count().to_pandas()
missing_table = missing_table.mutate(missing_prct=(missing_table.missing_count/count)*100)
missing_table = missing_table.order_by(missing_table.missing_prct.desc())
print(missing_table.head(5))

end = time.time()
print("Cell [9] time:", end-start)
print()
print()


# In[10]:

start = time.time()

table = table.drop('Number', 'Wind_Chill(F)')

end = time.time()
print("Cell [10] time:", end-start)
print()
print()


# In[11]:

start = time.time()

precipitation_is_null = table['Precipitation(in)'].isnull()
precipiation_na = precipitation_is_null.ifelse(1, 0)
table = table.mutate(Precipitation_NA = precipiation_na)
print(table.head())

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

col_new = table['Precipitation(in)'].fillna(table['Precipitation(in)'].approx_median())
table = table.mutate(Precipitation_in_nonan = col_new)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:


start = time.time()

table = table.drop('Precipitation(in)')

end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:

start = time.time()

table = table.dropna(subset=['City','Zipcode','Airport_Code',
                       'Sunrise_Sunset','Civil_Twilight','Nautical_Twilight','Astronomical_Twilight'])

end = time.time()
print("Cell [14] time:", end-start)
print()
print()


# In[15]:


# # Code from the original notebook -- Ibis only allows scalar values as replacement for fillna
# # group data by 'Airport_Code' and 'Start_Month' then fill NAs with median value
# Weather_data=['Temperature(F)','Humidity(%)','Pressure(in)','Visibility(mi)','Wind_Speed(mph)']
# print("The number of remaining missing values: ")
# for i in Weather_data:
#   df[i] = df.groupby(['Airport_Code','Month'])[i].apply(lambda x: x.fillna(x.median()))
#   print( i + " : " + df[i].isnull().sum().astype(str))


# In[16]:


start = time.time()

Weather_data=['Temperature(F)','Humidity(%)','Pressure(in)','Visibility(mi)','Wind_Speed(mph)']
table = table.dropna(subset=Weather_data)

end = time.time()
print("Cell [16] time:", end-start)
print()
print()

# In[17]:


# # LOOK INTO THIS, THIS SHOULD NOT TAKE 10 seconds
# start = time.time()

# print(table)

# end = time.time()
# print("Cell [17] time:", end-start)
# print()
# print()

# In[18]:


get_ipython().system('rm $dbname*')

