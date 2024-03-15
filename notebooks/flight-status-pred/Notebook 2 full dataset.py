#!/usr/bin/env python
# coding: utf-8

# https://www.kaggle.com/code/ahmedeltom/flights-delay-analysis-eda

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
dbname = "flight_status_pred.db"

# # Default format
# tablename = "data_2018"
# input_file = "data/Combined_Flights_2018.csv"
# schema1 = init_ddb_from_csv(dbname, tablename, input_file)

# tablename = "data_2019"
# input_file = "data/Combined_Flights_2019.csv"
# schema2 = init_ddb_from_csv(dbname, tablename, input_file)

# tablename = "data_2020"
# input_file = "data/Combined_Flights_2020.csv"
# schema3 = init_ddb_from_csv(dbname, tablename, input_file)

# tablename = "data_2021"
# input_file = "data/Combined_Flights_2021.csv"
# schema4 = init_ddb_from_csv(dbname, tablename, input_file)

# tablename = "data_2022"
# input_file = "data/Combined_Flights_2022.csv"
# schema5= init_ddb_from_csv(dbname, tablename, input_file)

# # Split format
tablename = "data_2018"
input_file = "split_dataset/Combined_Flights_2018"
schema1 = init_ddb_from_split_csv(dbname, tablename, input_file)

tablename = "data_2019"
input_file = "split_dataset/Combined_Flights_2019"
schema2 = init_ddb_from_split_csv(dbname, tablename, input_file)

tablename = "data_2020"
input_file = "split_dataset/Combined_Flights_2020"
schema3 = init_ddb_from_split_csv(dbname, tablename, input_file)

tablename = "data_2021"
input_file = "split_dataset/Combined_Flights_2021"
schema4 = init_ddb_from_split_csv(dbname, tablename, input_file)

tablename = "data_2022"
input_file = "split_dataset/Combined_Flights_2022"
schema5 = init_ddb_from_split_csv(dbname, tablename, input_file)

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

ibis.options.interactive = True

con = ibis.duckdb.connect(dbname)
for schema in [schema1, schema2, schema3, schema4, schema5]:
    con.register_schema(schema)
print(con.tables)

end = time.time()
print("Cell [3] time:", end-start)
print()
print()


# In[4]:


start = time.time()
data_2018 = con.table('data_2018')
print(data_2018.head())

end = time.time()
print("Cell [4] time:", end-start)
print()
print()



# In[5]:


start = time.time()
data_2019 = con.table('data_2019')
print(data_2019.head())
end = time.time()
print("Cell [5] time:", end-start)
print()
print()

start = time.time()
data_2020 = con.table('data_2020')
print(data_2020.head())

end = time.time()
print("Cell [4] time:", end-start)
print()
print()

start = time.time()
data_2021 = con.table('data_2021')
print(data_2021.head())

end = time.time()
print("Cell [4] time:", end-start)
print()
print()

start = time.time()
data_2022 = con.table('data_2022')
print(data_2022.head())

end = time.time()
print("Cell [4] time:", end-start)
print()
print()


# In[6]:


start = time.time()
data_2018 = data_2018["FlightDate",
    "Airline",
    "Tail_Number",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes"]

data_2019 = data_2019["FlightDate",
    "Airline",
    "Tail_Number",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes"]

data_2020 = data_2020["FlightDate",
    "Airline",
    "Tail_Number",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes"]

data_2021 = data_2021["FlightDate",
    "Airline",
    "Tail_Number",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes"]

data_2022 = data_2022["FlightDate",
    "Airline",
    "Tail_Number",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes"]
end = time.time()
print("Cell [6] time:", end-start)
print()
print()



# In[7]:


start = time.time()
print(data_2018.count(), len(data_2018.columns))
print(data_2019.count(), len(data_2019.columns))
print(data_2020.count(), len(data_2020.columns))
print(data_2021.count(), len(data_2021.columns))
print(data_2022.count(), len(data_2022.columns))
end = time.time()
print("Cell [7] time:", end-start)
print()
print()



# In[8]:

start = time.time()
group_by_airline = data_2018.group_by('Airline').aggregate(data_2018.count())
print(group_by_airline)
group_by_airline = data_2019.group_by('Airline').aggregate(data_2019.count())
print(group_by_airline)
group_by_airline = data_2020.group_by('Airline').aggregate(data_2020.count())
print(group_by_airline)
group_by_airline = data_2021.group_by('Airline').aggregate(data_2021.count())
print(group_by_airline)
group_by_airline = data_2022.group_by('Airline').aggregate(data_2022.count())
print(group_by_airline)
end = time.time()
print("Cell [8] time:", end-start)
print()
print()



# In[9]:


start = time.time()
data_2018 = data_2018.mutate(Delayed = False)

filter_exp = data_2018['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse(True, data_2018['Delayed'])
data_2108 = data_2018.mutate(Delayed=ifelse_exp)

end = time.time()
print("Cell [9] time:", end-start)
print()
print()



# In[10]:


start = time.time()
data_2018 = data_2018.mutate(OnTime = True)

filter_exp = data_2018['Delayed'] == True
ifelse_exp = filter_exp.ifelse(False, data_2018['OnTime'])
data_2018 = data_2018.mutate(OnTime=ifelse_exp)

end = time.time()
print("Cell [10] time:", end-start)
print()
print()



# In[11]:


start = time.time()
data_2019 = data_2019.mutate(Delayed = False)

filter_exp = data_2019['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse(True, data_2019['Delayed'])
data_2109 = data_2019.mutate(Delayed=ifelse_exp)

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

data_2019 = data_2019.mutate(OnTime = True)

filter_exp = data_2019['Delayed'] == True
ifelse_exp = filter_exp.ifelse(False, data_2019['OnTime'])
data_2019 = data_2019.mutate(OnTime=ifelse_exp)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()

# In[11]:


start = time.time()
data_2020 = data_2020.mutate(Delayed = False)

filter_exp = data_2020['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse(True, data_2020['Delayed'])
data_2120 = data_2020.mutate(Delayed=ifelse_exp)

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

data_2020 = data_2020.mutate(OnTime = True)

filter_exp = data_2020['Delayed'] == True
ifelse_exp = filter_exp.ifelse(False, data_2020['OnTime'])
data_2020 = data_2020.mutate(OnTime=ifelse_exp)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[11]:


start = time.time()
data_2021 = data_2021.mutate(Delayed = False)

filter_exp = data_2021['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse(True, data_2021['Delayed'])
data_2121 = data_2021.mutate(Delayed=ifelse_exp)

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

data_2021 = data_2021.mutate(OnTime = True)

filter_exp = data_2021['Delayed'] == True
ifelse_exp = filter_exp.ifelse(False, data_2021['OnTime'])
data_2021 = data_2021.mutate(OnTime=ifelse_exp)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[11]:


start = time.time()
data_2022 = data_2022.mutate(Delayed = False)

filter_exp = data_2022['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse(True, data_2022['Delayed'])
data_2122 = data_2022.mutate(Delayed=ifelse_exp)

end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:


start = time.time()

data_2022 = data_2022.mutate(OnTime = True)

filter_exp = data_2022['Delayed'] == True
ifelse_exp = filter_exp.ifelse(False, data_2022['OnTime'])
data_2022 = data_2022.mutate(OnTime=ifelse_exp)

end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:


start = time.time()

data_2018 = data_2018.mutate(Destination_City = data_2018['DestCityName'].split(', ')[0])
data_2018 = data_2018.mutate(Destination_State = data_2018['DestCityName'].split(', ')[1])
data_2018 = data_2018.mutate(Origin_City = data_2018['OriginCityName'].split(', ')[0])
data_2018 = data_2018.mutate(Origin_State = data_2018['OriginCityName'].split(', ')[1])
data_2018 = data_2018.drop('DestCityName', 'OriginCityName')

end = time.time()
print("Cell [13] time:", end-start)
print()
print()



# In[14]:

start = time.time()

data_2019 = data_2019.mutate(Destination_City = data_2019['DestCityName'].split(', ')[0])
data_2019 = data_2019.mutate(Destination_State = data_2019['DestCityName'].split(', ')[1])
data_2019 = data_2019.mutate(Origin_City = data_2019['OriginCityName'].split(', ')[0])
data_2019 = data_2019.mutate(Origin_State = data_2019['OriginCityName'].split(', ')[1])
data_2019 = data_2019.drop('DestCityName', 'OriginCityName')

end = time.time()
print("Cell [14] time:", end-start)
print()
print()

# In[14]:

start = time.time()

data_2020 = data_2020.mutate(Destination_City = data_2020['DestCityName'].split(', ')[0])
data_2020 = data_2020.mutate(Destination_State = data_2020['DestCityName'].split(', ')[1])
data_2020 = data_2020.mutate(Origin_City = data_2020['OriginCityName'].split(', ')[0])
data_2020 = data_2020.mutate(Origin_State = data_2020['OriginCityName'].split(', ')[1])
data_2020 = data_2020.drop('DestCityName', 'OriginCityName')

end = time.time()
print("Cell [14] time:", end-start)
print()
print()

# In[14]:

start = time.time()

data_2021 = data_2021.mutate(Destination_City = data_2021['DestCityName'].split(', ')[0])
data_2021 = data_2021.mutate(Destination_State = data_2021['DestCityName'].split(', ')[1])
data_2021 = data_2021.mutate(Origin_City = data_2021['OriginCityName'].split(', ')[0])
data_2021 = data_2021.mutate(Origin_State = data_2021['OriginCityName'].split(', ')[1])
data_2021 = data_2021.drop('DestCityName', 'OriginCityName')

end = time.time()
print("Cell [14] time:", end-start)
print()
print()

# In[14]:

start = time.time()

data_2022 = data_2022.mutate(Destination_City = data_2022['DestCityName'].split(', ')[0])
data_2022 = data_2022.mutate(Destination_State = data_2022['DestCityName'].split(', ')[1])
data_2022 = data_2022.mutate(Origin_City = data_2022['OriginCityName'].split(', ')[0])
data_2022 = data_2022.mutate(Origin_State = data_2022['OriginCityName'].split(', ')[1])
data_2022 = data_2022.drop('DestCityName', 'OriginCityName')

end = time.time()
print("Cell [14] time:", end-start)
print()
print()


# In[15]:


start = time.time()

print(data_2018.head())

end = time.time()
print("Cell [15] time:", end-start)
print()
print()



# In[16]:

start = time.time()

print(data_2019.head())

end = time.time()
print("Cell [16] time:", end-start)
print()
print()

# In[16]:

start = time.time()

print(data_2020.head())

end = time.time()
print("Cell [16] time:", end-start)
print()
print()


# In[16]:

start = time.time()

print(data_2021.head())

end = time.time()
print("Cell [16] time:", end-start)
print()
print()


# In[16]:

start = time.time()

print(data_2022.head())

end = time.time()
print("Cell [16] time:", end-start)
print()
print()

# In[17]:


start = time.time()

delayed_flights = data_2018[data_2018.Delayed == True]
print(delayed_flights)
cancelled_flights = data_2018[data_2018.Cancelled == True]
print(cancelled_flights)

end = time.time()
print("Cell [17] time:", end-start)
print()
print()



# In[18]:


start = time.time()

delayed_flights = data_2019[data_2019.Delayed == True]
print(delayed_flights)
cancelled_flights = data_2019[data_2019.Cancelled == True]
print(cancelled_flights)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()


# In[18]:


start = time.time()

delayed_flights = data_2020[data_2020.Delayed == True]
print(delayed_flights)
cancelled_flights = data_2020[data_2020.Cancelled == True]
print(cancelled_flights)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()

# In[18]:


start = time.time()

delayed_flights = data_2021[data_2021.Delayed == True]
print(delayed_flights)
cancelled_flights = data_2021[data_2021.Cancelled == True]
print(cancelled_flights)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()

# In[18]:


start = time.time()

delayed_flights = data_2022[data_2022.Delayed == True]
print(delayed_flights)
cancelled_flights = data_2022[data_2022.Cancelled == True]
print(cancelled_flights)

end = time.time()
print("Cell [18] time:", end-start)
print()
print()

# In[19]:


start = time.time()

flights_group_by_airline = data_2018.group_by('Airline').aggregate(data_2018.count())
print(flights_group_by_airline)

end = time.time()
print("Cell [19] time:", end-start)
print()
print()


# In[20]:


start = time.time()

print(flights_group_by_airline)

end = time.time()
print("Cell [20] time:", end-start)
print()
print()



# In[21]:


start = time.time()

flights_group_by_airline = data_2020.group_by('Airline').aggregate(data_2020.count())

end = time.time()
print("Cell [21] time:", end-start)
print()
print()



# In[22]:


start = time.time()

print(flights_group_by_airline)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()

# In[21]:


start = time.time()

flights_group_by_airline = data_2021.group_by('Airline').aggregate(data_2021.count())

end = time.time()
print("Cell [21] time:", end-start)
print()
print()



# In[22]:


start = time.time()

print(flights_group_by_airline)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()

# In[21]:


start = time.time()

flights_group_by_airline = data_2022.group_by('Airline').aggregate(data_2022.count())

end = time.time()
print("Cell [21] time:", end-start)
print()
print()



# In[22]:


start = time.time()

print(flights_group_by_airline)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()

# In[21]:


start = time.time()

flights_group_by_airline = data_2019.group_by('Airline').aggregate(data_2019.count())

end = time.time()
print("Cell [21] time:", end-start)
print()
print()



# In[22]:


start = time.time()

print(flights_group_by_airline)

end = time.time()
print("Cell [22] time:", end-start)
print()
print()


# In[23]:


# After this point the notebook renames columns, it is not clear which column goes where


# In[24]:


get_ipython().system('rm $dbname')

