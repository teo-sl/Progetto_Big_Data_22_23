# variabili globali
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

import os.path
import numpy as np
import pickle

STARTING_MONTH = 1
ENDING_MONTH = 9
STARTING_DAY_OF_MONTH = 1

column_aliases = {"DEST_STATE_FULL_NAME": "Destination state", "ORIGIN_STATE_FULL_NAME": "Origin state", "DEST_AIRPORT_FULL_NAME": "Destination airport", 
                    "ORIGIN_AIRPORT_FULL_NAME": "Origin airport"}

column_per_aggregation_level = {"Daily": "FlightDate", "Weekly": "WeekofMonth", "Monthly": "Month"}

spark = SparkSession.builder.appName("flights").getOrCreate()

def get_column_aliases():
    return column_aliases

def get_column_alias_key(value):
    for key in column_aliases.keys():
        if (column_aliases[key] == value):
            return key

def get_column_per_agg_level():
    return column_per_aggregation_level


def get_destinations(destination_type):
    file_path = "code/backend/serialized_objects/" + destination_type + ".pkl"
    loaded_destinations = None
    with open(file_path, "rb") as f:
        loaded_destinations = pickle.load(f)
    return loaded_destinations        

def get_origins(origin_type):
    file_path = "code/backend/serialized_objects/" + origin_type + ".pkl"
    loaded_origins = None
    with open(file_path, 'rb') as f:
        loaded_origins = pickle.load(f)
    return loaded_origins



# aggiungere lo schema
def load_dataset():
    flights_df = spark.read.csv("data.nosync/cleaned/cleaned_flights.csv", inferSchema=True, header=True)
    return flights_df

def load_cache():
    # check if util/cache.pkl exists
    try:
        cache = pickle.load(open("util/cache.pkl","rb"))
    except:
        cache = {}
    return cache


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


