###### IMPORTS ########


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from pyspark.sql.types import StructType
import pickle
from multiprocessing.pool import ThreadPool

###### CONSTANTS ########

spark = SparkSession.builder.appName("flights").getOrCreate()
cancelled_diverted = pd.read_csv("util/cancelled_diverted.csv")
cancelled_diverted['FlightDate'] = pd.to_datetime(cancelled_diverted['FlightDate'])

###### UTIL FUNCTIONS ########

# cache management

def load_cache():
    # check if util/cache.pkl exists
    # if not create it
    try:
        cache = pickle.load(open("util/cache.pkl","rb"))
    except:
        cache = {}
    return cache

# dataset retrieval

def load_dataset():
    with open("util/schema.json","r") as f:
        schema = StructType.fromJson(json.load(f))
    
    
    df = spark.read.csv("data.nosync/cleaned/cleaned_flights.csv",
                                            schema=schema, header=True)

    return df

# get the unique dates from pickle

def get_dates():
    dates = pickle.load(open("util/dates.pkl","rb"))
    return dates


###### QUERIES ########

# heatmap query

def matrix_agg(df,x,y,z="count"):
    if z=="count":
        df_aggregated = df.groupBy(x,y).agg({"*": "count"}).\
                                    withColumnRenamed(f"count(1)", f"{z}_agg")
    else:
        df_aggregated = df.groupBy(x,y).agg({z: "avg"}).\
                                    withColumnRenamed(f"avg({z})", f"{z}_agg")
    return df_aggregated

# pie chart query


def origin_dest_query(df,from_date,to_date,query="ArrDelay"):
    # filter the dataframe using timestamp from_date and to_date
    df = df.filter(df["FlightDate"].between(from_date,to_date))
    
    if query=="count":
        df = df.groupBy("ORIGIN_STATE","DEST_STATE").agg({"*": "count"}).\
                                    withColumnRenamed("count(1)", "count")
    else:
        df = df.groupBy("ORIGIN_STATE","DEST_STATE").agg({"ArrDelay": "avg"}).\
                                withColumnRenamed("avg(ArrDelay)", "ArrDelay")

    # order by query, descendant order
    df = df.orderBy(df[query].desc())

    return df

# map routes query

def routes_queries(df,date_start,date_end,origin="BOS",query="NumFlights",scope="airports"):
    # filter 
    df_aggregated = df.filter((col("Origin") == origin))
    df_aggregated = df_aggregated.filter((col("FlightDate") >= date_start) & 
                                                    (col("FlightDate") <= date_end))

    # group by
    if scope == "airports":
        df_aggregated = df_aggregated.\
                    groupBy("Origin","Dest","ORIGIN_LATITUDE","ORIGIN_LONGITUDE",
                                    "DEST_LATITUDE","DEST_LONGITUDE").\
                    agg({"ArrDelay": "avg","*":"count"}).\
                    withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay").\
                    withColumnRenamed("count(1)", "NumFlights")
    else:
        df_aggregated = df_aggregated.groupBy("ORIGIN_STATE","DEST_STATE").\
                    agg({"ArrDelay": "avg","*":"count","ORIGIN_LATITUDE":"avg",
                        "DEST_LATITUDE":"avg","ORIGIN_LONGITUDE":"avg",
                        "DEST_LONGITUDE":"avg"}).\
                    withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay").\
                    withColumnRenamed("count(1)", "NumFlights")

        # rename columns
        df_aggregated = df_aggregated.withColumnRenamed("avg(ORIGIN_LATITUDE)", "ORIGIN_LATITUDE").\
                                    withColumnRenamed("avg(ORIGIN_LONGITUDE)", "ORIGIN_LONGITUDE").\
                                    withColumnRenamed("avg(DEST_LATITUDE)", "DEST_LATITUDE").\
                                    withColumnRenamed("avg(DEST_LONGITUDE)", "DEST_LONGITUDE")

    # sort by query and take the first 100 rows
    df_aggregated = df_aggregated.orderBy(df_aggregated[query].desc()).limit(100)

    return df_aggregated


# states map query

def states_map_query(df,group):
    df_aggregated = df.groupBy(group).\
                        agg({"ArrDelay": "avg", "*":"count"}).\
                        withColumnRenamed("avg(ArrDelay)", "ArrDelay")

    df_aggregated = df_aggregated.withColumnRenamed("count(1)","count")
    return df_aggregated

# reporting airlines query

def reporting_airlines_queries(df,from_date,to_date,query="count"):
    # get the tuples in between the dates
    df = df.filter(df["FlightDate"].between(from_date,to_date))

    if query == "count":
        df_agg = df.groupBy("Reporting_Airline").\
            agg({"*": "count"}).\
            withColumnRenamed("count(1)", "count").\
            orderBy("count", ascending=False)
    elif query == "Cancelled":
        df_agg = df.groupBy("Reporting_Airline").\
            agg({"Cancelled": "sum"}).\
            withColumnRenamed("sum(Cancelled)", "Cancelled").\
            orderBy("Cancelled", ascending=False)
    else:
        df_agg=df.groupBy("Reporting_Airline").\
            agg({"ArrDelay": "avg"}).\
            withColumnRenamed("avg(ArrDelay)", "ArrDelay").\
            orderBy("ArrDelay", ascending=False)        
    
    return df_agg

# scatter plot query

def scatter_queries(df,temp_granularity):
    df_agg =(df.groupBy(temp_granularity).\
    agg({"ArrDelay": "avg","TaxiIn":"avg","TaxiOut":"avg","DepDelay":"avg",
            "DepTime":"avg","ArrTime":"avg","AirTime":"avg","Distance":"avg",
            "*":"count"}).\
        withColumnRenamed("count(1)", "count").\
        withColumnRenamed("avg(ArrDelay)", "ArrDelay").\
        withColumnRenamed("avg(TaxiIn)", "TaxiIn").\
        withColumnRenamed("avg(TaxiOut)", "TaxiOut").\
        withColumnRenamed("avg(DepDelay)", "DepDelay").\
        withColumnRenamed("avg(AirTime)", "AirTime").\
        withColumnRenamed("avg(Distance)", "Distance"))
    return df_agg

# textual query

def textual_queries(df,from_date,to_date):
    df = df.filter(df["FlightDate"].between(from_date,to_date))
    
    cd_filtered = cancelled_diverted[(cancelled_diverted['FlightDate'] >= from_date) & 
                    (cancelled_diverted['FlightDate'] <= to_date)]
    
    cancelled = cd_filtered['Cancelled'].sum()
    diverted = cd_filtered['Diverted'].sum()

    # execute the queries in parallel
    pool = ThreadPool(4)
    num = pool.apply_async(lambda : df.count())
    airports = pool.apply_async(lambda : df.select('Origin').
                                            union(df.select('Dest')).
                                            distinct().count())
    delayed = pool.apply_async(lambda : df.filter(df["ArrDelay"] > 0).count())
    average_delay = pool.apply_async(lambda : df.agg({"ArrDelay": "avg"}).
                                                            collect()[0][0])
    pool.close()

    pool.join()

    # get the results
    num = num.get()
    airports = airports.get()
    delayed = delayed.get()
    average_delay = average_delay.get()

    return [num,airports,cancelled,delayed,diverted,average_delay]
