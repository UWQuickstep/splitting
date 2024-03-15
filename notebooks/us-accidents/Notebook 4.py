#!/usr/bin/env python
# coding: utf-8

# This notebook is re-implementing the data manipulation portions of the most voted notebook https://www.kaggle.com/code/nikitagrec/usa-accidents-plotly-maps-text-classification on Kaggle for the US-Accidents dataset. The original notebook is written using the `pandas` library, here we are re-writing only the data manipulation operations (omitting plotting, training, etc.) in `ibis` with **DuckDB** backend.
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
count_by_state = table.group_by('State').aggregate(table.count())
print(count_by_state)
end = time.time()
print("Cell [6] time:", end-start)
print()
print()


# In[7]:


start = time.time()
count_by_county = table.group_by('County').aggregate(table.count())
print(count_by_county)
end = time.time()
print("Cell [7] time:", end-start)
print()
print()


# In[8]:


start = time.time()
table_10000 = table.head(10000)
end = time.time()
print("Cell [8] time:", end-start)
print()
print()

# In[9]:


start = time.time()
severity_by_county = table.group_by(['County']).aggregate(table.Severity.mean())
print(severity_by_county)
end = time.time()
print("Cell [9] time:", end-start)
print()
print()


# In[10]:


start = time.time()
table_dropna = table.dropna(['Start_Lng','Start_Lat','City','Visibility(mi)','Severity'])
print(table_dropna.count())
end = time.time()
print("Cell [10] time:", end-start)
print()
print()


# In[11]:


start = time.time()
table_dropna = table.dropna(['Start_Lng','Start_Lat','City','Distance(mi)','Severity'])
print(table_dropna.count())
end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()
text_data = table[['Description', 'Severity']].to_pandas()
end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:


start = time.time()
print(text_data.head())
end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:


get_ipython().system('rm $dbname*')

