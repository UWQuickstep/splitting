#!/usr/bin/env python
# coding: utf-8

# https://www.kaggle.com/code/argha48/preliminary-data-visualization

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
    read_csv_args_list = ["AUTO_DETECT = TRUE"]
    for key, value in kwargs.items():
        read_csv_args_list.append("{0} = {1}".format(key, value))
    read_csv_args = ','.join(read_csv_args_list)
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
            sql_stmt = "CREATE TABLE {} AS SELECT * FROM read_csv('{}', {})".format(sub_tablename, full_filename, read_csv_args)
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

dbname = "nyc_parking_tickets.db"

# # Default format
tablename = "tickets_2014"
input_file = "nyc_parking_tickets/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"
schema = init_ddb_from_csv(dbname, tablename, input_file, SAMPLE_SIZE=-1)

# # Split format
# tablename = "tickets_2014"
# input_file = "nyc_parking_tickets_split/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_"
# schema = init_ddb_from_split_csv(dbname, tablename, input_file, SAMPLE_SIZE=-1)

end = time.time()
print("Cell [2] time:", end-start)
print()
print()


# In[3]:


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
print(con.tables)

end = time.time()
print("Cell [3] time:", end-start)
print()
print()


# In[4]:

start = time.time()

tickets_2014 = con.table('tickets_2014')
print(tickets_2014.head())

end = time.time()
print("Cell [4] time:", end-start)
print()
print()



# In[5]:

start = time.time()

print(tickets_2014.count(), len(tickets_2014.columns))

end = time.time()
print("Cell [5] time:", end-start)
print()
print()



# In[6]:

start = time.time()

for col in tickets_2014.columns:
    nunique = tickets_2014[col].nunique().to_pandas()
    isnull = tickets_2014[col].isnull().sum().to_pandas()
    print (col, nunique, isnull)

end = time.time()
print("Cell [6] time:", end-start)
print()
print()


# In[7]:

start = time.time()

tickets_2014 = tickets_2014.drop('No Standing or Stopping Violation', 'Hydrant Violation',
               'Double Parking Violation', 'Latitude', 'Longitude',
               'Community Board', 'Census Tract', 'BIN',
               'BBL', 'NTA',
               'Street Code1', 'Street Code2', 'Street Code3','Meter Number', 'Violation Post Code',
                'Law Section', 'Sub Division', 'House Number', 'Street Name')

end = time.time()
print("Cell [7] time:", end-start)
print()
print()


# In[8]:

start = time.time()

tickets_2014 = tickets_2014.dropna(subset=['Plate ID'])

end = time.time()
print("Cell [8] time:", end-start)
print()
print()

# In[9]:

start = time.time()

tickets_2014['Plate ID'].isnull().sum()

end = time.time()
print("Cell [9] time:", end-start)
print()
print()

# In[10]:


start = time.time()
print(tickets_2014.count(), len(tickets_2014.columns))
end = time.time()
print("Cell [10] time:", end-start)
print()
print()



# In[11]:

start = time.time()

group_by_registration_state = tickets_2014.group_by('Registration State').aggregate(tickets_2014.count())

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:

start = time.time()

print(group_by_registration_state)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()

# In[13]:


start= time.time()

tickets_2014 = tickets_2014.mutate(month=tickets_2014['Issue Date'].month())
group_by_month = tickets_2014.group_by(tickets_2014['month']).aggregate(tickets_2014.count())
print(group_by_month)

end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:


start = time.time()
group_by_violation_code =  tickets_2014.group_by(tickets_2014['Violation Code']).aggregate(tickets_2014.count())
print(group_by_violation_code)

end = time.time()
print("Cell [14] time:", end-start)
print()
print()



# In[15]:


start = time.time()
group_by_body_type = tickets_2014.group_by(tickets_2014['Vehicle Body Type']).aggregate(tickets_2014.count())
print(group_by_body_type)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()



# In[16]:

start = time.time()

gorup_by_make =  tickets_2014.group_by(tickets_2014['Vehicle Make']).aggregate(tickets_2014.count())
print(gorup_by_make)

end = time.time()
print("Cell [16] time:", end-start)
print()
print()



# In[17]:


# Cannot currently apply custom functions to columns in ibis


# In[18]:

start = time.time()

group_by_county = tickets_2014.group_by(tickets_2014['Violation County']).aggregate(tickets_2014.count())
print(group_by_county)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()



# In[19]:


start = time.time()

print(tickets_2014['Unregistered Vehicle?'].nunique())

end = time.time()
print("Cell [19] time:", end-start)
print()
print()



# In[20]:


start = time.time()

group_by_vehicle_year =  tickets_2014.group_by(tickets_2014['Vehicle Year']).aggregate(tickets_2014.count())
print(group_by_vehicle_year)

end = time.time()
print("Cell [20] time:", end-start)
print()
print()



# In[21]:


start = time.time()

group_by_front_or_opposite =  tickets_2014.group_by(tickets_2014['Violation In Front Of Or Opposite']).aggregate(tickets_2014.count())
print(group_by_front_or_opposite)

end = time.time()
print("Cell [21] time:", end-start)
print()
print()


# In[22]:


get_ipython().system('rm $dbname*')

