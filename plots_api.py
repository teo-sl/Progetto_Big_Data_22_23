import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plotly.subplots import make_subplots
import plotly.offline as py

from spark_api import matrix_agg, origin_dest_query

week_days_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
months_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
states = pd.read_csv("https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv")


def matrix_plot(df,x,y,z="count"):
    df_pd = matrix_agg(df,x,y,z).toPandas()


    fig = px.imshow(
        df_pd.pivot(y, x, f"{z}_agg"), 
        labels=dict(x=x, y=y, color=f"{z}_agg"),
        y=week_days_names,
        x= months_names if x=="Month" else df_pd[x].unique().sort(),
    )

    return fig

def origin_dest_plot(df,from_date,to_date,query="ArrDelay"):
    df_pd = origin_dest_query(df,from_date,to_date,query).toPandas()

    fig = px.pie(df_pd.head(20), values=query, names='Origin-Dest', title=f'{query} by Origin-Dest')
    return fig


def origin_dest_plot(df,from_date,to_date,query="ArrDelay"):
    df_pd = origin_dest_query(df,from_date,to_date,query).toPandas()
    # make a join over STATE_ORIGIN and Abbreviation in states dataframe, rename the columns
    df_pd = df_pd.merge(states, left_on="ORIGIN_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Origin"}).\
        merge(states, left_on="DEST_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Dest"})
    # create a new column with the origin and destination
    df_pd["Origin-Dest"] = df_pd["Origin"] + " - " + df_pd["Dest"]
    fig = px.pie(df_pd.head(20), values=query, names='Origin-Dest', title=f'{query} by Origin-Dest')
    return fig