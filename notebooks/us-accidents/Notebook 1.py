#!/usr/bin/env python
# coding: utf-8

# This notebook is re-implementing the data manipulation portions of the most voted notebook https://www.kaggle.com/code/deepakdeepu8978/how-severity-the-accidents-is on Kaggle for the US-Accidents dataset. The original notebook is written using the `pandas` library, here we are re-writing only the data manipulation operations (omitting plotting, training, etc.) in `ibis` with **DuckDB** backend.
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

# Default -- loading directly from CSV
csv_filename = "US_Accidents_Dec21_updated.csv"
schema = init_ddb_from_csv(dbname, tablename, csv_filename)

# # # Split -- loading from split CSV
# split_csv = "US_Accidents_Dec21_updated_split"
# schema = init_ddb_from_split_csv(dbname, tablename, split_csv)

end = time.time()
print("Cell [2] time:", end-start)
print()
print()

# In[3]:


schema


# In[4]:


start = time.time()

import numpy as np
import pandas as pd
import ibis
import os

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

shape = (table.count(), len(table.columns))
print(shape)
print(table.head())

end = time.time()
print("Cell [6] time:", end-start)
print()
print()


# In[7]:


start = time.time()

count_by_state = table.group_by('State').aggregate(table.count())
print(count_by_state)

end = time.time()
print("Cell [7] time:", end-start)
print()
print()


# In[8]:


start = time.time()

states = count_by_state['State'].collect()
counts = count_by_state['CountStar(accidents)'].collect()

# Ommitted plotting
print(states, counts)

end = time.time()
print("Cell [8] time:", end-start)
print()
print()


# In[9]:


start = time.time()

start_lat = table.Start_Lat.collect()
start_lng = table.Start_Lng.collect()

# Skipping plotting

end = time.time()
print("Cell [9] time:", end-start)
print()
print()


# In[10]:


start = time.time()

end_lat = table.End_Lat.collect()
end_lng = table.End_Lng.collect()

# Skipping plotting

end = time.time()
print("Cell [10] time:", end-start)
print()
print()


# In[11]:


start = time.time()

top_weather_conds = table['Weather_Condition'].value_counts().order_by(ibis.desc("Weather_Condition_count")).head(5)
xlabels = top_weather_conds.Weather_Condition.collect()
y = top_weather_conds.Weather_Condition_count.collect()

# top_weather_conds

# Skipping plotting
print(xlabels, y)

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

col_names = table.columns
col_types = []
for col in col_names:
    col_types.append(table[col].type().name)
type_df = pd.DataFrame({"col_name": col_names, "types": col_types})
type_table = ibis.memtable(type_df, name="type_table")
type_table.types.value_counts()

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:


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
missing_table = missing_table.mutate(missing_ratio=missing_table.missing_count/count)
missing_table[missing_table.missing_ratio > 0.777]

end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:


start = time.time()

print(missing_table[missing_table.column_name == 'Astronomical_Twilight'])

end = time.time()
print("Cell [14] time:", end-start)
print()
print()


# In[15]:

start = time.time()

missin = missing_table[missing_table.missing_count > 250000]
remove_list = missin['column_name'].collect()
print(remove_list)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()


# In[16]:


start = time.time()

print(table.count())

end = time.time()
print("Cell [16] time:", end-start)
print()
print()


# In[17]:


# # # Code from the original notebook in pandas
# # train_df['Start_Time'] = pd.to_datetime(train_df['Start_Time'], errors='coerce')
# # train_df['End_Time'] = pd.to_datetime(train_df['End_Time'], errors='coerce')

# # Start_Time and End_Time are already parsed as timestamp types
# table = table.mutate(Start_Time=table.Start_Time.to_timestamp('%Y-%m-%d %H:%M:%S'))
# table = table.mutate(End_Time=table.End_Time.to_timestamp('%Y-%m-%d %H:%M:%S'))


# In[18]:

start = time.time()

# # Code from the original notebook in pandas
# # Extract year, month, day, hour and weekday
# train_df['Year']=train_df['Start_Time'].dt.year
# train_df['Month']=train_df['Start_Time'].dt.strftime('%b')
# train_df['Day']=train_df['Start_Time'].dt.day
# train_df['Hour']=train_df['Start_Time'].dt.hour
# train_df['Weekday']=train_df['Start_Time'].dt.strftime('%a')

table = table.mutate(Year=table.Start_Time.year())
table = table.mutate(Month=table.Start_Time.month())
table = table.mutate(Day=table.Start_Time.day())
table = table.mutate(Hour=table.Start_Time.hour())
table = table.mutate(Weekday=table.Start_Time.day_of_week.index())

end = time.time()
print("Cell [18] time:", end-start)
print()
print()


# In[19]:


# # Code from the original notebook in pandas
# # Extract the amount of time in the unit of minutes for each accident, round to the nearest integer
# td='Time_Duration(min)'
# train_df[td]=round((train_df['End_Time']-train_df['Start_Time'])/np.timedelta64(1,'m'))

# # Currently throws a translation error, not sure how to do this in ibis right now
# table = table.mutate(Time_Duration_min=(table.End_Time-table.Start_Time).minutes)


# In[20]:


# # Drop rows with negative td
# train_df.dropna(subset=[td],axis=0,inplace=True)

# table = table[table.Time_Duration_min > 0]


# In[21]:


start = time.time()

float_cols = type_table[type_table.types == 'Float64'].col_name.collect().to_pandas()
labels = []
values = []
print(float_cols)
for col in float_cols:
    if col == 'Severity':
        continue
    # For some reason, there is a None column in table
    if not col:
        break
    labels.append(col)
    # Ibis doesn't maintain the ordering of rows, so the following doesn't work
    # values.append(np.corrcoef(table[col.to_pandas()].to_pandas(), table['Severity'].to_pandas()))
    temp_df = table.select(col, "Severity").to_pandas()
    values.append(np.corrcoef(temp_df[col], temp_df['Severity']))

end = time.time()
print("Cell [21] time:", end-start)
print()
print()

# Skipping plotting


# In[22]:


start = time.time()

print(labels, values)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()

# In[ ]:


# Cleanup
get_ipython().system('rm $dbname*')

