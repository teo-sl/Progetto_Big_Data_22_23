import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plotly.subplots import make_subplots
import plotly.offline as py

from spark_api import week_day_month_agg



def week_day_matrix_avg_delay(df):
    df_pd = week_day_month_agg(df).toPandas()
    fig = px.imshow(
        df_pd.pivot("DayOfWeek", "Month", "AverageArrivalDelay"), 
        labels=dict(x="Month", y="DayOfWeek", color="Average Arrival Delay"), 
        x=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 
        y=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    )
    # add title "Average delay by day of week and month"
    fig.update_layout(title="Average delay by day of week and month")
    return fig

