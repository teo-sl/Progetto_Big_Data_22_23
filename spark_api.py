###### IMPORTS ########


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from pyspark.sql.types import StructType
import pickle

###### CONSTANTS ########

spark = SparkSession.builder.appName("flights").getOrCreate()
cancelled_diverted = pd.read_csv("util/cancelled_diverted.csv")
cancelled_diverted['FlightDate'] = pd.to_datetime(cancelled_diverted['FlightDate'])
textual = pd.read_csv("util/textual_queries.csv")
textual['FlightDate'] = pd.to_datetime(textual['FlightDate'])


column_aliases = {"DEST_STATE_FULL_NAME": "Destination state", "ORIGIN_STATE_FULL_NAME": "Origin state", "DEST_AIRPORT_FULL_NAME": "Destination airport", 
                    "ORIGIN_AIRPORT_FULL_NAME": "Origin airport"}

column_per_aggregation_level = {"Daily": "FlightDate", "Weekly": "WeekofMonth", "Monthly": "Month"}

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

def get_column_aliases():
    return column_aliases

def get_column_alias_key(value):
    for key in column_aliases.keys():
        if (column_aliases[key] == value):
            return key

def get_column_per_agg_level():
    return column_per_aggregation_level


def get_destinations(destination_type):
    file_path = "serialized_objects/" + destination_type + ".pkl"
    loaded_destinations = None
    with open(file_path, "rb") as f:
        loaded_destinations = pickle.load(f)
    return loaded_destinations        

def get_origins(origin_type):
    file_path = "serialized_objects/" + origin_type + ".pkl"
    loaded_origins = None
    with open(file_path, 'rb') as f:
        loaded_origins = pickle.load(f)
    return loaded_origins



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
    # filter textual pandas dataframe using the dates
    textual_filtered = textual[(textual['FlightDate'] >= from_date) &
                                (textual['FlightDate'] <= to_date)]
    
    num = textual_filtered["count"].sum()
    delayed = textual_filtered["delay_count"].sum()
    average_delay = textual_filtered["delay_sum"].sum()/num
    
    cd_filtered = cancelled_diverted[(cancelled_diverted['FlightDate'] >= from_date) & 
                    (cancelled_diverted['FlightDate'] <= to_date)]
    
    cancelled = cd_filtered['Cancelled'].sum()
    diverted = cd_filtered['Diverted'].sum()



    return [num,cancelled,delayed,diverted,average_delay]

# plot della classifica dei primi x migliori in base allo stato di destinazione o aereporto di destinazione. 
def compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by): 
    flights_per_places = flights_df.filter((start_month <= flights_df.Month) \
                                            & (flights_df.Month <= end_month) \
                                            & (start_day <= flights_df.DayofMonth) \
                                            & (flights_df.DayofMonth <= end_day)).\
                                    select(col(place_attribute)).\
                                    groupBy(col(place_attribute)).\
                                    count()

    if sort_by == "Top":
        flights_per_places = flights_per_places.sort(col("count").desc())
    else:
        flights_per_places = flights_per_places.sort(col("count").asc())
                                    
    # prendo le prime top x destinazioni 
    places = flights_per_places.limit(x)
    # Rinomino le colonne per rendere il grafico più comprensibile
    places = places.withColumnRenamed("count", "Count")

    return places


def compute_flights_per_place(flights_df, start_month, end_month, start_day, end_day, place_attribute):
    flights_per_place_column = flights_df.filter((start_month <= flights_df.Month) \
                                            & (flights_df.Month <= end_month) \
                                            & (start_day <= flights_df.DayofMonth) \
                                            & (flights_df.DayofMonth <= end_day)).\
                                    select(col(place_attribute), "FlightDate").\
                                    groupBy(col(place_attribute), "FlightDate").\
                                    count().\
                                    orderBy(col("FlightDate"))
    return flights_per_place_column

def compute_flights_per_selected_place(flights_df, place_column, place):
    flights_per_selected_place = flights_df.filter((flights_df[place_column] == place)).\
                                            select("FlightDate").\
                                            groupBy("FlightDate").\
                                            count().\
                                            orderBy(col("FlightDate"))
    return flights_per_selected_place
    

def compute_mean_arr_delay_per_dest(flights_df, destinations, dest_attribute, aggregation_level):
    period = column_per_aggregation_level[aggregation_level]
    mean_arr_delay_per_dest = flights_df.filter(flights_df[dest_attribute].isin(destinations)).\
                                    select(col(dest_attribute), period, "ArrDelayMinutes").\
                                    groupBy(col(dest_attribute), period).\
                                    agg({"ArrDelayMinutes": "avg"}).\
                                    orderBy(col(period))

    return mean_arr_delay_per_dest
    

def compute_mean_dep_delay_per_origin(flights_df, origins, origin_attribute, aggregation_level):
    period = column_per_aggregation_level[aggregation_level]

    mean_dep_delay_per_origin = flights_df.filter(flights_df[origin_attribute].isin(origins)).\
                                    select(col(origin_attribute), period, "DepDelayMinutes").\
                                    groupBy(col(origin_attribute), period).\
                                    agg({"DepDelayMinutes": "avg"}).\
                                    orderBy(col(period))

    return mean_dep_delay_per_origin
