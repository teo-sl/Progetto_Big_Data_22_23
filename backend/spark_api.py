# variabili globali
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import numpy as np

STARTING_MONTH = 1
ENDING_MONTH = 9
STARTING_DAY_OF_MONTH = 1

column_aliases = {"DEST_STATE": "Destination state", "ORIGIN_STATE": "Origin state", "Dest": "Destination airport", 
                    "Origin": "Origin airport"}

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

def get_destinations(flights_df, destination_type):
    return list(flights_df.select(destination_type).distinct().toPandas()[destination_type])

def get_origins(flights_df, origin_type):
    return list(flights_df.select(origin_type).distinct().toPandas()[origin_type])

# aggiungere lo schema
def load_dataset():
    flights_df = spark.read.csv("data.nosync/cleaned/cleaned_flights.csv", inferSchema=True, header=True)
    return flights_df


# plot della classifica dei primi x migliori in base allo stato di destinazione o aereporto di destinazione. 
def compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by): 
    place_attributes = ["Dest", "Origin", "DEST_STATE", "ORIGIN_STATE"]
    
    if place_attribute not in place_attributes:
        raise Exception("place_attribute must be one of the following : ", place_attribute)

    if x < 0 or x > flights_df.count():
        raise Exception("X cannot be negative or bigger than dataframe size!")

    if start_month > ENDING_MONTH or end_month < STARTING_MONTH or start_month < 0 or end_month < 0:
        raise Exception("Please specificy a well formed month interval")

    if (start_day > end_day) or (end_day < start_day) or (start_day < 0) or (end_day < 0):
        raise Exception("Please specificy a well formed day interval")
    
    

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
    flights_per_place = flights_df.filter((start_month <= flights_df.Month) \
                                            & (flights_df.Month <= end_month) \
                                            & (start_day <= flights_df.DayofMonth) \
                                            & (flights_df.DayofMonth <= end_day)).\
                                    select(col(place_attribute), "FlightDate").\
                                    groupBy(col(place_attribute), "FlightDate").\
                                    count().\
                                    orderBy(col("FlightDate"))

    return flights_per_place
    

def compute_mean_arr_delay_per_dest(flights_df, destinations, dest_attribute, aggregation_level):
    if not ((dest_attribute == "Dest") or (dest_attribute == "DEST_STATE")):
        raise Exception("Place attribute must be Dest or DEST_STATE")
    
    period = column_per_aggregation_level[aggregation_level]
    mean_arr_delay_per_dest = flights_df.filter(flights_df[dest_attribute].isin(destinations)).\
                                    select(col(dest_attribute), period, "ArrDelayMinutes").\
                                    groupBy(col(dest_attribute), period).\
                                    agg({"ArrDelayMinutes": "avg"}).\
                                    orderBy(col(period))

    return mean_arr_delay_per_dest
    

def compute_mean_dep_delay_per_origin(flights_df, origins, origin_attribute, aggregation_level):
    if not ((origin_attribute == "Origin") or (origin_attribute == "ORIGIN_STATE")):
        raise Exception("Place attribute must be Origin or ORIGIN_STATE")
        
    period = column_per_aggregation_level[aggregation_level]

    mean_dep_delay_per_origin = flights_df.filter(flights_df[origin_attribute].isin(origins)).\
                                    select(col(origin_attribute), period, "DepDelayMinutes").\
                                    groupBy(col(origin_attribute), period).\
                                    agg({"DepDelayMinutes": "avg"}).\
                                    orderBy(col(period))

    return mean_dep_delay_per_origin


def compute_delay_groups(flights_df, destination, dest_attribute, aggregation_level):
    if not ((dest_attribute == "Dest") or (dest_attribute == "DEST_STATE")):
        raise Exception("Dest attribute must be either Dest or Dest state")

    period = column_per_aggregation_level[aggregation_level]

    delay_groups = flights_df.filter(flights_df[dest_attribute] == destination).\
                                    select(col(dest_attribute), period, "DepDel15").\
                                    groupBy(col(dest_attribute), period, "DepDel15").\
                                    count().\
                                    orderBy(col(period))
    
    return delay_groups


def compute_delay_matrix(flights_df, airport_dest):
    matrix = flights_df.filter(flights_df.Dest.isin(airport_dest)).\
                        select("Dest", "ArrDelayMinutes", "DepDelayMinutes", "Month", "DayofWeek").\
                        groupBy("DayofWeek","Month", "Dest").\
                        agg({"ArrDelayMinutes": "avg", "DepDelayMinutes": "avg"})
                        
    matrix = matrix.withColumnRenamed("avg(ArrDelayMinutes)", "Avg arrival delay").\
                    withColumnRenamed("avg(DepDelayMinutes)", "Avg departure delay").\
                    withColumnRenamed("Dest", "Destination airport")            

    return matrix 



def facet_plot_over_interval(flights_df, start_month, end_month, start_day, end_day, places, place_attribute):
    place_attributes = ["Dest", "Origin", "DEST_STATE", "ORIGIN_STATE"]
    
    if place_attribute not in place_attributes:
        raise Exception("place_attribute must be one of the following : ", place_attribute)

    if start_month > ENDING_MONTH or end_month < STARTING_MONTH or start_month < 0 or end_month < 0:
        raise Exception("Please specificy a well formed month interval")

    if (start_day > end_day) or (end_day < start_day) or (start_day < 0) or (end_day < 0):
        raise Exception("Please specificy a well formed day interval")


    flights_df = flights_df.filter((start_month <= flights_df.Month) \
                                            & (flights_df.Month <= end_month) \
                                            & (start_day <= flights_df.DayofMonth) \
                                            & (flights_df.DayofMonth <= end_day)\
                                            & (flights_df[place_attribute].isin(places))).\
                                            select(col(place_attribute), "FlightDate").\
                                            groupBy(col(place_attribute), "FlightDate").\
                                            count().\
                                            orderBy(col("FlightDate"))
    
    fig = make_subplots(rows=len(places), cols=1) 

    for i in range(len(places)):
        flights_per_state_pd = flights_df.filter(flights_df[place_attribute] == places[i]).toPandas()
        fig.add_trace(
            go.Scatter(x=flights_per_state_pd['FlightDate'], y=flights_per_state_pd['count'], name=places[i]),
            row=i+1, col=1
        )
        
    word = "from" if ((place_attribute == "Origin") or (place_attribute == "ORIGIN_STATE")) else "to"   
    place_column_alias = column_aliases[place_attribute]
    title = "Daily number of flighs " + word + " " + place_column_alias
    fig.update_layout(height=1100, width=1200, title_text="Daily number of flights to states")
    fig.show()   



