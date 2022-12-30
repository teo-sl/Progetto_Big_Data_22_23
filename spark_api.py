import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from pyspark.sql.types import StructType
import pickle


spark = SparkSession.builder.appName("flights").getOrCreate()



def load_dataset():
    with open("util/schema.json","r") as f:
        schema = StructType.fromJson(json.load(f))
    
    
    df = spark.read.csv("data.nosync/cleaned/cleaned_flights.csv",schema=schema, header=True)
    # sample 10% of the data
    #df = df.sample(False, 0.1, seed=42)

    return df

# get the unique dates
def get_dates():
    dates = pickle.load(open("util/dates.pkl","rb"))
    return dates

# get the unique airports in Origin and Dest
def get_airports(df):
    # get the unique airports in Origin
    airports_origin = df.select("Origin").distinct().orderBy("Origin", ascending=True).toPandas()["Origin"]
    # get the unique airports in Dest
    airports_dest = df.select("Dest").distinct().orderBy("Dest", ascending=True).toPandas()["Dest"]
    # get the unique airports
    airports = pd.concat([airports_origin, airports_dest]).unique()
    return airports



def matrix_agg(df,x,y,z="count",):
    if z=="count":
        df_aggregated = df.groupBy(x,y).agg({"*": "count"}).withColumnRenamed(f"count(1)", f"{z}_agg")
    else:
        df_aggregated = df.groupBy(x,y).agg({z: "avg"}).withColumnRenamed(f"avg({z})", f"{z}_agg")
    return df_aggregated

def origin_dest_query(df,from_date,to_date,query="ArrDelay"):
    df = df.filter((df["FlightDate"] >= from_date) & (df["FlightDate"] <= to_date))
    if query=="count":
        df = df.groupBy("Origin","Dest").agg({"*": "count"}).withColumnRenamed("count(1)", "count")
    else:
        df = df.groupBy("Origin","Dest").agg({"ArrDelay": "avg"}).withColumnRenamed("avg(ArrDelay)", "ArrDelay")

    # crate a new column with the origin and destination
    df = df.withColumn("Origin-Dest", concat(df["Origin"], lit("-"), df["Dest"]))
    return df


def origin_dest_query(df,from_date,to_date,query="ArrDelay"):
    # filter the dataframe using timestamp from_date and to_date
    df = df.filter(df["FlightDate"].between(from_date,to_date))
    
    if query=="count":
        df = df.groupBy("ORIGIN_STATE","DEST_STATE").agg({"*": "count"}).withColumnRenamed("count(1)", "count")
    else:
        df = df.groupBy("ORIGIN_STATE","DEST_STATE").agg({"ArrDelay": "avg"}).withColumnRenamed("avg(ArrDelay)", "ArrDelay")

    # crate a new column with the origin and destination
    # order by query, descendant order
    df = df.orderBy(df[query].desc())

    return df

def routes_queries(df,date_start,date_end,origin="BOS",query="NumFlights",scope="airports"):
    df_aggregated = df.filter((col("Origin") == origin))
    df_aggregated = df_aggregated.filter((col("FlightDate") >= date_start) & (col("FlightDate") <= date_end))
    # aggregate by flight date, day of weew, Origin and Dest, count the number of flights and average the arrival delay
    if scope == "airports":
        df_aggregated = df_aggregated.groupBy("Origin","Dest","ORIGIN_LATITUDE","ORIGIN_LONGITUDE","DEST_LATITUDE","DEST_LONGITUDE").agg({"ArrDelay": "avg","*":"count"}).withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay").withColumnRenamed("count(1)", "NumFlights")
    else:
        df_aggregated = df_aggregated.groupBy("ORIGIN_STATE","DEST_STATE").agg({"ArrDelay": "avg","*":"count","ORIGIN_LATITUDE":"avg","DEST_LATITUDE":"avg","ORIGIN_LONGITUDE":"avg","DEST_LONGITUDE":"avg"}).withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay").withColumnRenamed("count(1)", "NumFlights")
        # rename columns
        df_aggregated = df_aggregated.withColumnRenamed("avg(ORIGIN_LATITUDE)", "ORIGIN_LATITUDE").withColumnRenamed("avg(ORIGIN_LONGITUDE)", "ORIGIN_LONGITUDE").withColumnRenamed("avg(DEST_LATITUDE)", "DEST_LATITUDE").withColumnRenamed("avg(DEST_LONGITUDE)", "DEST_LONGITUDE")

    # sort by query and take the first 100 rows
    df_aggregated = df_aggregated.orderBy(df_aggregated[query].desc()).limit(100)
    return df_aggregated

def states_map_query(df,group):
    df_aggregated = df.groupBy(group).agg({"ArrDelay": "avg", "*":"count"}).withColumnRenamed("avg(ArrDelay)", "ArrDelay")
    df_aggregated = df_aggregated.withColumnRenamed("count(1)","count")
    return df_aggregated

def reporting_airlines_queries(df,from_date,to_date,query="count"):
    # get the tuples in between the dates
    df = df.filter(df["FlightDate"].between(from_date,to_date))

    if query == "count":
        df_agg = df.groupBy("Reporting_Airline").\
            agg({"*": "count"}).\
            withColumnRenamed("count(1)", "count").\
            orderBy("count", ascending=False)
    else:
        df_agg=df.groupBy("Reporting_Airline").\
            agg({"ArrDelay": "avg"}).\
            withColumnRenamed("avg(ArrDelay)", "ArrDelay").\
            orderBy("ArrDelay", ascending=False)        
    
    return df_agg

def scatter_queries(df,temp_granularity):
    df_agg =(df.groupBy(temp_granularity).\
    agg({"ArrDelay": "avg","TaxiIn":"avg","TaxiOut":"avg","DepDelay":"avg","DepTime":"avg",
        "ArrTime":"avg","AirTime":"avg","Distance":"avg","*":"count"}).\
        withColumnRenamed("count(1)", "count").\
        withColumnRenamed("avg(ArrDelay)", "ArrDelay").\
        withColumnRenamed("avg(TaxiIn)", "TaxiIn").\
        withColumnRenamed("avg(TaxiOut)", "TaxiOut").\
        withColumnRenamed("avg(DepDelay)", "DepDelay").\
        withColumnRenamed("avg(AirTime)", "AirTime").\
        withColumnRenamed("avg(Distance)", "Distance"))
    return df_agg
