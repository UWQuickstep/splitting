#!/usr/bin/env python
# coding: utf-8

# This notebook is re-implementing the data manipulation portions of the most voted notebook https://www.kaggle.com/code/satyabrataroy/60-insights-extraction-us-accident-analysis on Kaggle for the US-Accidents dataset. The original notebook is written using the `pandas` library, here we are re-writing only the data manipulation operations (omitting plotting, training, etc.) in `ibis` with **DuckDB** backend.
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

end = time.time()
print("Cell [6] time:", end-start)
print()
print()


# In[7]:


start = time.time()

count_by_city = table.group_by("City").aggregate(table.count())
count_by_city.order_by(ibis.desc('CountStar(accidents)')).head(10)

end = time.time()
print("Cell [7] time:", end-start)
print()
print()


# In[8]:


start = time.time()

highest_cases = count_by_city.order_by(ibis.desc('CountStar(accidents)')).head(1).to_pandas()['CountStar(accidents)']
print(round(highest_cases/5))
print(round(highest_cases/(5*365)))

end = time.time()
print("Cell [8] time:", end-start)
print()
print()


# In[9]:


# Skipped plotting of top 10 cities


# In[10]:


start = time.time()

def city_cases_percentage(val, operator):
    if operator == '<':
        res = count_by_city[count_by_city['CountStar(accidents)'] < val].count()
    elif operator == '>':
        res = count_by_city[count_by_city['CountStar(accidents)'] > val].count()
    elif operator == '=':
        res = count_by_city[count_by_city['CountStar(accidents)'] == val].count()
    res = res.to_pandas()
    print(f'{res} Cities, {round(res*100/count_by_city.count().to_pandas(), 2)}%')

city_cases_percentage(1, '=')
city_cases_percentage(100, '<')
city_cases_percentage(1000, '<')
city_cases_percentage(1000, '>')
city_cases_percentage(5000, '>')
city_cases_percentage(10000, '>')

end = time.time()
print("Cell [10] time:", end-start)
print()
print()


# In[11]:


start = time.time()

# Top 10 states with most accidents
count_by_state = table.group_by(table.State).aggregate(table.count())
print(count_by_state.order_by(ibis.desc('CountStar(accidents)')).head(10))

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

# Top 10 states with least accidents
print(count_by_state.order_by('CountStar(accidents)').head(10))

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:


# Count by timezone
start = time.time()

count_by_timezone = table.group_by(table.Timezone).aggregate(table.count())
print(count_by_timezone)

end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:


start = time.time()

# Count by street
count_by_street = table.group_by(table.Street).aggregate(table.count())
print(count_by_street.order_by(ibis.desc('CountStar(accidents)')).head(10))

end = time.time()
print("Cell [14] time:", end-start)
print()
print()


# In[15]:


start = time.time()

def street_cases_percentage(val, operator):
    if operator == '=':
        val = count_by_street[count_by_street['CountStar(accidents)'] == val].count()
    elif operator == '>':
        val = count_by_street[count_by_street['CountStar(accidents)'] > val].count()
    elif operator == '<':
        val = count_by_street[count_by_street['CountStar(accidents)'] < val].count()
    val = val.to_pandas()
    print('{:,d} Streets, {}%'.format(val, round(val*100/count_by_street.count().to_pandas(), 2)))
    
    
street_cases_percentage(1, '=')
street_cases_percentage(100, '<')
street_cases_percentage(1000, '<')
street_cases_percentage(1000, '>')
street_cases_percentage(5000, '>')

end = time.time()
print("Cell [15] time:", end-start)
print()
print()


# In[16]:


start = time.time()

# Count by severity
count_by_severity = table.group_by(table.Severity).aggregate(table.count())
print(count_by_severity)

end = time.time()
print("Cell [16] time:", end-start)
print()
print()


# In[17]:


# Not sure how to do accident duration analysis
# table.Start_Time - table.End_Time


# In[18]:


start = time.time()

# Count by year
count_by_year = table.group_by(table.Start_Time.year()).aggregate(table.count())
print(count_by_year)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()


# In[19]:

start = time.time()

count_by_year_severity = table.group_by([table.Start_Time.year(), table.Severity]).aggregate(table.count())
print(count_by_year_severity)

end = time.time()
print("Cell [19] time:", end-start)
print()
print()


# In[20]:


start = time.time()

count_by_year = count_by_year.mutate(accident_per_day=count_by_year['CountStar(accidents)']/(365))
count_by_hour = count_by_year.mutate(accident_per_hour=count_by_year['CountStar(accidents)']/(365*5))

end = time.time()
print("Cell [20] time:", end-start)
print()
print()

# In[21]:

start = time.time()

# Count by month
count_by_month = table.group_by(table.Start_Time.month()).aggregate(table.count())
print(count_by_month)

end = time.time()
print("Cell [21] time:", end-start)
print()
print()


# In[22]:


start = time.time()

# Count by day
count_by_day = table.group_by(table.Start_Time.day_of_week.index()).aggregate(table.count())
print(count_by_day)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()


# In[23]:


start = time.time()

# Count by Hour
count_by_hour = table.group_by(table.Start_Time.hour()).aggregate(table.count())
print(count_by_hour)

end = time.time()
print("Cell [23] time:", end-start)
print()
print()


# In[24]:


start = time.time()

# Road conditions
print(table.Bump.value_counts())
print(table.Crossing.value_counts())
print(table.Give_Way.value_counts())
print(table.Junction.value_counts())
print(table.Stop.value_counts())
print(table.No_Exit.value_counts())
print(table.Traffic_Signal.value_counts())
print(table.Turning_Loop.value_counts())

end = time.time()
print("Cell [24] time:", end-start)
print()
print()


# In[25]:


start = time.time()

count_by_temp = table.group_by('Temperature(F)').aggregate(table.count())
print(count_by_temp['Temperature(F)'].to_pandas())

end = time.time()
print("Cell [25] time:", end-start)
print()
print()


# In[26]:


start = time.time()

count_by_humidity = table.group_by('Humidity(%)').aggregate(table.count())
print(count_by_humidity['Humidity(%)'].to_pandas())

end = time.time()
print("Cell [26] time:", end-start)
print()
print()


# In[27]:


start = time.time()

count_by_pressure = table.group_by('Pressure(in)').aggregate(table.count())
print(count_by_pressure['Pressure(in)'].to_pandas())

end = time.time()
print("Cell [27] time:", end-start)
print()
print()


# In[28]:


start = time.time()

count_by_wind_chill = table.group_by('Wind_Chill(F)').aggregate(table.count())
print(count_by_wind_chill['Wind_Chill(F)'].to_pandas())

end = time.time()
print("Cell [28] time:", end-start)
print()
print()


# In[29]:


start = time.time()

count_by_wind_speed = table.group_by('Wind_Speed(mph)').aggregate(table.count())
print(count_by_wind_speed['Wind_Speed(mph)'].to_pandas())

end = time.time()
print("Cell [29] time:", end-start)
print()
print()


# In[30]:

start = time.time()

count_by_visibility = table.group_by('Visibility(mi)').aggregate(table.count())
print(count_by_visibility['Visibility(mi)'].to_pandas())

end = time.time()
print("Cell [30] time:", end-start)
print()
print()


# In[31]:


start = time.time()

print(table)

end = time.time()
print("Cell [31] time:", end-start)
print()
print()


# In[32]:

start = time.time()

table = table.dropna()
print(table.count())

end = time.time()
print("Cell [32] time:", end-start)
print()
print()


# Cleanup
get_ipython().system('rm $dbname*')

