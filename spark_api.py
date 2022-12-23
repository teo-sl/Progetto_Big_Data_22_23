import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plotly.subplots import make_subplots
import plotly.offline as py
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType


spark = SparkSession.builder.appName("flights").getOrCreate()


def load_dataset():
    with open("schema.json","r") as f:
        schema = StructType.fromJson(json.load(f))
    df = spark.read.csv("data.nosync/cleaned/cleaned_flights.csv",schema=schema, header=True)
    return df



def week_day_month_agg(df):
    df_aggregated = df.groupBy("DayOfWeek", "Month").agg({"ArrDelay": "avg"}).withColumnRenamed("avg(ArrDelay)", "AverageArrivalDelay")
    return df_aggregated