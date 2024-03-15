#!/usr/bin/env python
# coding: utf-8

# https://www.kaggle.com/code/robikscube/flight-delay-exploratory-data-analysis-twitch

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

# Default format
tablename = "data_2018"
input_file = "data/Combined_Flights_2018.csv"
schema1 = init_ddb_from_csv(dbname, tablename, input_file)

tablename = "data_2019"
input_file = "data/Combined_Flights_2019.csv"
schema2 = init_ddb_from_csv(dbname, tablename, input_file)

tablename = "data_2020"
input_file = "data/Combined_Flights_2020.csv"
schema3 = init_ddb_from_csv(dbname, tablename, input_file)

tablename = "data_2021"
input_file = "data/Combined_Flights_2021.csv"
schema4 = init_ddb_from_csv(dbname, tablename, input_file)

tablename = "data_2022"
input_file = "data/Combined_Flights_2022.csv"
schema5 = init_ddb_from_csv(dbname, tablename, input_file)

# # Split format
# tablename = "data_2018"
# input_file = "split_dataset/Combined_Flights_2018"
# schema1 = init_ddb_from_split_csv(dbname, tablename, input_file)

# tablename = "data_2019"
# input_file = "split_dataset/Combined_Flights_2019"
# schema2 = init_ddb_from_split_csv(dbname, tablename, input_file)

# tablename = "data_2020"
# input_file = "split_dataset/Combined_Flights_2020"
# schema3 = init_ddb_from_split_csv(dbname, tablename, input_file)

# tablename = "data_2021"
# input_file = "split_dataset/Combined_Flights_2021"
# schema4 = init_ddb_from_split_csv(dbname, tablename, input_file)

# tablename = "data_2022"
# input_file = "split_dataset/Combined_Flights_2022"
# schema5 = init_ddb_from_split_csv(dbname, tablename, input_file)

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

# In[5]:

start = time.time()

data_2020 = con.table('data_2020')
print(data_2020.head())

end = time.time()
print("Cell [5] time:", end-start)
print()
print()

# In[5]:

start = time.time()

data_2021 = con.table('data_2021')
print(data_2021.head())

end = time.time()
print("Cell [5] time:", end-start)
print()
print()

# In[5]:

start = time.time()

data_2022 = con.table('data_2022')
print(data_2022.head())

end = time.time()
print("Cell [5] time:", end-start)
print()
print()



# In[6]:


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


# In[7]:


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


# In[8]:

start = time.time()
print(data_2018.schema())

end = time.time()
print("Cell [8] time:", end-start)
print()
print()


# In[9]:

start = time.time()
print(data_2019.schema())

end = time.time()
print("Cell [9] time:", end-start)
print()
print()

# In[9]:

start = time.time()
print(data_2020.schema())

end = time.time()
print("Cell [9] time:", end-start)
print()
print()

# In[9]:

start = time.time()
print(data_2021.schema())

end = time.time()
print("Cell [9] time:", end-start)
print()
print()

# In[9]:

start = time.time()
print(data_2022.schema())

end = time.time()
print("Cell [9] time:", end-start)
print()
print()


# In[10]:

start = time.time()
group_by_airline = data_2018.group_by('Airline').aggregate(data_2018.count())
print(group_by_airline)
end = time.time()
print("Cell [10] time:", end-start)
print()
print()


# In[11]:

start = time.time()
group_by_airline = data_2019.group_by('Airline').aggregate(data_2019.count())
print(group_by_airline)
end = time.time()
print("Cell [11] time:", end-start)
print()
print()

# In[11]:

start = time.time()
group_by_airline = data_2020.group_by('Airline').aggregate(data_2020.count())
print(group_by_airline)
end = time.time()
print("Cell [11] time:", end-start)
print()
print()

# In[11]:

start = time.time()
group_by_airline = data_2021.group_by('Airline').aggregate(data_2021.count())
print(group_by_airline)
end = time.time()
print("Cell [11] time:", end-start)
print()
print()

# In[11]:

start = time.time()
group_by_airline = data_2022.group_by('Airline').aggregate(data_2022.count())
print(group_by_airline)
end = time.time()
print("Cell [11] time:", end-start)
print()
print()


# In[12]:

start = time.time()
flights_for_plotting = data_2018[data_2018['DepDelayMinutes'] < 30]
print(flights_for_plotting.head())
end = time.time()
print("Cell [12] time:", end-start)
print()
print()


# In[13]:

start = time.time()
# flights_for_plotting = data_2019[data_2019['DepDelayMinutes'] < 30]
# print(flights_for_plotting.head())
end = time.time()
print("Cell [13] time:", end-start)
print()
print()

start = time.time()
# flights_for_plotting = data_2020[data_2020['DepDelayMinutes'] < 30]
# print(flights_for_plotting.head())
end = time.time()
print("Cell [13] time:", end-start)
print()
print()

start = time.time()
# flights_for_plotting = data_2021[data_2021['DepDelayMinutes'] < 30]
# print(flights_for_plotting.head())
end = time.time()
print("Cell [13] time:", end-start)
print()
print()

start = time.time()
# flights_for_plotting = data_2022[data_2022['DepDelayMinutes'] < 30]
# print(flights_for_plotting.head())
end = time.time()
print("Cell [13] time:", end-start)
print()
print()


# In[14]:

start = time.time()
cancelled_flights = data_2018.group_by('Cancelled').aggregate(data_2018.count())
print(cancelled_flights)

end = time.time()
print("Cell [14] time:", end-start)
print()
print()



# In[15]:

start = time.time()
cancelled_flights = data_2019.group_by('Cancelled').aggregate(data_2019.count())
print(cancelled_flights)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()

start = time.time()
cancelled_flights = data_2020.group_by('Cancelled').aggregate(data_2020.count())
print(cancelled_flights)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()

start = time.time()
cancelled_flights = data_2021.group_by('Cancelled').aggregate(data_2021.count())
print(cancelled_flights)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()

start = time.time()
cancelled_flights = data_2022.group_by('Cancelled').aggregate(data_2022.count())
print(cancelled_flights)

end = time.time()
print("Cell [15] time:", end-start)
print()
print()



# In[16]:


start = time.time()
diverted_flights = data_2018.group_by('Diverted').aggregate(data_2018.count())
print(diverted_flights)

diverted_flights = data_2019.group_by('Diverted').aggregate(data_2019.count())
print(diverted_flights)

diverted_flights = data_2020.group_by('Diverted').aggregate(data_2020.count())
print(diverted_flights)

diverted_flights = data_2021.group_by('Diverted').aggregate(data_2021.count())
print(diverted_flights)

diverted_flights = data_2022.group_by('Diverted').aggregate(data_2022.count())
print(diverted_flights)

end = time.time()
print("Cell [16] time:", end-start)
print()
print()



# In[17]:


# filter_exp = table.Wind_Direction_0 == 'West'
# ifelse_exp = filter_exp.ifelse('W', table.Wind_Direction_0)
# table = table.mutate(Wind_Direction_1 = ifelse_exp)
# table = table.drop('Wind_Direction_0')

start = time.time()

data_2018 = data_2018.mutate(delay_group=None)

filter_exp = data_2018['DepDelayMinutes'] == 0
ifelse_exp = filter_exp.ifelse('OnTime_Early', data_2018.delay_group)
data_2018 = data_2018.mutate(delay_group = ifelse_exp)

filter_exp = data_2018['DepDelayMinutes'] > 0
ifelse_exp = filter_exp.ifelse('Small_Delay', data_2018.delay_group)
data_2018 = data_2018.mutate(delay_group = ifelse_exp)

filter_exp = data_2018['DepDelayMinutes'] > 15
ifelse_exp = filter_exp.ifelse('Mediaum_Delay', data_2018.delay_group)
data_2018 = data_2018.mutate(delay_group = ifelse_exp)

filter_exp = data_2018['DepDelayMinutes'] > 45
ifelse_exp = filter_exp.ifelse('Large_Delay', data_2018.delay_group)
data_2018 = data_2018.mutate(delay_group = ifelse_exp)

filter_exp = data_2018['Cancelled'] == True
ifelse_exp = filter_exp.ifelse('Cancelled', data_2018.delay_group)
data_2018 = data_2018.mutate(delay_group = ifelse_exp)

end = time.time()
print("Cell [17] time:", end-start)
print()
print()



# In[18]:

start = time.time()
print(data_2018.head())
end = time.time()
print("Cell [18] time:", end-start)
print()
print()



# In[19]:


# filter_exp = table.Wind_Direction_0 == 'West'
# ifelse_exp = filter_exp.ifelse('W', table.Wind_Direction_0)
# table = table.mutate(Wind_Direction_1 = ifelse_exp)
# table = table.drop('Wind_Direction_0')

# start = time.time()

# data_2019 = data_2019.mutate(delay_group=None)

# filter_exp = data_2019['DepDelayMinutes'] == 0
# ifelse_exp = filter_exp.ifelse('OnTime_Early', data_2019.delay_group)
# data_2019 = data_2019.mutate(delay_group = ifelse_exp)

# filter_exp = data_2019['DepDelayMinutes'] > 0
# ifelse_exp = filter_exp.ifelse('Small_Delay', data_2019.delay_group)
# data_2019 = data_2019.mutate(delay_group = ifelse_exp)

# filter_exp = data_2019['DepDelayMinutes'] > 15
# ifelse_exp = filter_exp.ifelse('Mediaum_Delay', data_2019.delay_group)
# data_2019 = data_2019.mutate(delay_group = ifelse_exp)

# filter_exp = data_2019['DepDelayMinutes'] > 45
# ifelse_exp = filter_exp.ifelse('Large_Delay', data_2019.delay_group)
# data_2019 = data_2019.mutate(delay_group = ifelse_exp)

# filter_exp = data_2019['Cancelled'] == True
# ifelse_exp = filter_exp.ifelse('Cancelled', data_2019.delay_group)
# data_2019 = data_2019.mutate(delay_group = ifelse_exp)

# end = time.time()
# print("Cell [19] time:", end-start)
# print()
# print()

# start = time.time()

# data_2020 = data_2020.mutate(delay_group=None)

# filter_exp = data_2020['DepDelayMinutes'] == 0
# ifelse_exp = filter_exp.ifelse('OnTime_Early', data_2020.delay_group)
# data_2020 = data_2020.mutate(delay_group = ifelse_exp)

# filter_exp = data_2020['DepDelayMinutes'] > 0
# ifelse_exp = filter_exp.ifelse('Small_Delay', data_2020.delay_group)
# data_2020 = data_2020.mutate(delay_group = ifelse_exp)

# filter_exp = data_2020['DepDelayMinutes'] > 15
# ifelse_exp = filter_exp.ifelse('Mediaum_Delay', data_2020.delay_group)
# data_2020 = data_2020.mutate(delay_group = ifelse_exp)

# filter_exp = data_2020['DepDelayMinutes'] > 45
# ifelse_exp = filter_exp.ifelse('Large_Delay', data_2020.delay_group)
# data_2020 = data_2020.mutate(delay_group = ifelse_exp)

# filter_exp = data_2020['Cancelled'] == True
# ifelse_exp = filter_exp.ifelse('Cancelled', data_2020.delay_group)
# data_2020 = data_2020.mutate(delay_group = ifelse_exp)

# end = time.time()
# print("Cell [19] time:", end-start)
# print()
# print()

# start = time.time()

# data_2021 = data_2021.mutate(delay_group=None)

# filter_exp = data_2021['DepDelayMinutes'] == 0
# ifelse_exp = filter_exp.ifelse('OnTime_Early', data_2021.delay_group)
# data_2021 = data_2021.mutate(delay_group = ifelse_exp)

# filter_exp = data_2021['DepDelayMinutes'] > 0
# ifelse_exp = filter_exp.ifelse('Small_Delay', data_2021.delay_group)
# data_2021 = data_2021.mutate(delay_group = ifelse_exp)

# filter_exp = data_2021['DepDelayMinutes'] > 15
# ifelse_exp = filter_exp.ifelse('Mediaum_Delay', data_2021.delay_group)
# data_2021 = data_2021.mutate(delay_group = ifelse_exp)

# filter_exp = data_2021['DepDelayMinutes'] > 45
# ifelse_exp = filter_exp.ifelse('Large_Delay', data_2021.delay_group)
# data_2021 = data_2021.mutate(delay_group = ifelse_exp)

# filter_exp = data_2021['Cancelled'] == True
# ifelse_exp = filter_exp.ifelse('Cancelled', data_2021.delay_group)
# data_2021 = data_2021.mutate(delay_group = ifelse_exp)

# end = time.time()
# print("Cell [19] time:", end-start)
# print()
# print()

# start = time.time()

# data_2022 = data_2022.mutate(delay_group=None)

# filter_exp = data_2022['DepDelayMinutes'] == 0
# ifelse_exp = filter_exp.ifelse('OnTime_Early', data_2022.delay_group)
# data_2022 = data_2022.mutate(delay_group = ifelse_exp)

# filter_exp = data_2022['DepDelayMinutes'] > 0
# ifelse_exp = filter_exp.ifelse('Small_Delay', data_2022.delay_group)
# data_2022 = data_2022.mutate(delay_group = ifelse_exp)

# filter_exp = data_2022['DepDelayMinutes'] > 15
# ifelse_exp = filter_exp.ifelse('Mediaum_Delay', data_2022.delay_group)
# data_2022 = data_2022.mutate(delay_group = ifelse_exp)

# filter_exp = data_2022['DepDelayMinutes'] > 45
# ifelse_exp = filter_exp.ifelse('Large_Delay', data_2022.delay_group)
# data_2022 = data_2022.mutate(delay_group = ifelse_exp)

# filter_exp = data_2022['Cancelled'] == True
# ifelse_exp = filter_exp.ifelse('Cancelled', data_2022.delay_group)
# data_2022 = data_2022.mutate(delay_group = ifelse_exp)

# end = time.time()
# print("Cell [19] time:", end-start)
# print()
# print()



# In[20]:

start = time.time()

print(data_2018.head())
print(data_2019.head())
print(data_2020.head())
print(data_2021.head())
print(data_2022.head())

end = time.time()
print("Cell [20] time:", end-start)
print()
print()



# In[21]:


start = time.time()
group_by_delay_group = data_2018.group_by('delay_group').aggregate(data_2018.count())
print(group_by_delay_group)
end = time.time()
print("Cell [21] time:", end-start)
print()
print()



# In[22]:


# start = time.time()
# group_by_delay_group = data_2019.group_by('delay_group').aggregate(data_2019.count())
# print(group_by_delay_group)
# end = time.time()
# print("Cell [22] time:", end-start)
# print()
# print()

# start = time.time()
# group_by_delay_group = data_2020.group_by('delay_group').aggregate(data_2020.count())
# print(group_by_delay_group)
# end = time.time()
# print("Cell [22] time:", end-start)
# print()
# print()

# start = time.time()
# group_by_delay_group = data_2021.group_by('delay_group').aggregate(data_2021.count())
# print(group_by_delay_group)
# end = time.time()
# print("Cell [22] time:", end-start)
# print()
# print()

# start = time.time()
# group_by_delay_group = data_2022.group_by('delay_group').aggregate(data_2022.count())
# print(group_by_delay_group)
# end = time.time()
# print("Cell [22] time:", end-start)
# print()
# print()



# In[23]:

start = time.time()
group_by_diverted_flights = data_2018.group_by('Diverted').aggregate(data_2018.count())
print(group_by_diverted_flights)
group_by_diverted_flights = data_2019.group_by('Diverted').aggregate(data_2019.count())
print(group_by_diverted_flights)
group_by_diverted_flights = data_2020.group_by('Diverted').aggregate(data_2020.count())
print(group_by_diverted_flights)
group_by_diverted_flights = data_2021.group_by('Diverted').aggregate(data_2021.count())
print(group_by_diverted_flights)
group_by_diverted_flights = data_2022.group_by('Diverted').aggregate(data_2022.count())
print(group_by_diverted_flights)
end = time.time()
print("Cell [23] time:", end-start)
print()
print()



# In[24]:

start = time.time()
data_2018 = data_2018.mutate(month=data_2018['FlightDate'].month())
data_2019 = data_2019.mutate(month=data_2019['FlightDate'].month())
data_2020 = data_2020.mutate(month=data_2020['FlightDate'].month())
data_2021 = data_2021.mutate(month=data_2021['FlightDate'].month())
data_2022 = data_2022.mutate(month=data_2022['FlightDate'].month())
end = time.time()
print("Cell [24] time:", end-start)
print()
print()



# In[25]:


start = time.time()
group_by_month_delay_group = data_2018.group_by(['month', 'delay_group']).aggregate(data_2018.count())
print(group_by_month_delay_group)
# group_by_month_delay_group = data_2019.group_by(['month', 'delay_group']).aggregate(data_2019.count())
# print(group_by_month_delay_group)
# group_by_month_delay_group = data_2020.group_by(['month', 'delay_group']).aggregate(data_2020.count())
# print(group_by_month_delay_group)
# group_by_month_delay_group = data_2021.group_by(['month', 'delay_group']).aggregate(data_2021.count())
# print(group_by_month_delay_group)
# group_by_month_delay_group = data_2022.group_by(['month', 'delay_group']).aggregate(data_2022.count())
# print(group_by_month_delay_group)
end = time.time()
print("Cell [25] time:", end-start)
print()
print()



# In[26]:


start = time.time()
group_by_flightdate_cancelled = data_2018.group_by(['FlightDate', 'Cancelled']).aggregate(data_2018['Cancelled'].sum())
print(group_by_flightdate_cancelled)
group_by_flightdate_cancelled = data_2019.group_by(['FlightDate', 'Cancelled']).aggregate(data_2019['Cancelled'].sum())
print(group_by_flightdate_cancelled)
group_by_flightdate_cancelled = data_2020.group_by(['FlightDate', 'Cancelled']).aggregate(data_2020['Cancelled'].sum())
print(group_by_flightdate_cancelled)
group_by_flightdate_cancelled = data_2021.group_by(['FlightDate', 'Cancelled']).aggregate(data_2021['Cancelled'].sum())
print(group_by_flightdate_cancelled)
group_by_flightdate_cancelled = data_2022.group_by(['FlightDate', 'Cancelled']).aggregate(data_2022['Cancelled'].sum())
print(group_by_flightdate_cancelled)
end = time.time()
print("Cell [26] time:", end-start)
print()
print()

# In[27]:


get_ipython().system('rm $dbname')

