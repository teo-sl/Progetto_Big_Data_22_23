import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plotly.subplots import make_subplots
import plotly.offline as py

from spark_api import matrix_agg, origin_dest_query, routes_queries

week_days_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
months_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
states = pd.read_csv("states.csv",delimiter="\t",header=None)
states.columns = ["State","unk","Abbreviation"]
airports = pd.read_csv("preprocessing/airports.csv")


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



def plot_routes(df,date_start,date_to,origin="BOS",query="NumFlights",scope="airports"):
    df_aggregated=routes_queries(df,date_start,date_to,origin,query,scope).toPandas()
    if scope == "airports":
        df_aggregated = df_aggregated.merge(airports, left_on="Origin", right_on="IATA")
        df_aggregated = df_aggregated.merge(airports, left_on="Dest", right_on="IATA")
    else:
        # join with states
        df_aggregated = df_aggregated.merge(states, left_on="ORIGIN_STATE", right_on="Abbreviation")
        df_aggregated = df_aggregated.merge(states, left_on="DEST_STATE", right_on="Abbreviation")
        
    
    fig = go.Figure()
    



    source_to_dest = zip(df_aggregated["ORIGIN_LATITUDE"], df_aggregated["DEST_LATITUDE"],
                         df_aggregated["ORIGIN_LONGITUDE"], df_aggregated["DEST_LONGITUDE"],
                         df_aggregated[query])

    ## Loop thorugh each flight entry to add line between source and destination
    for slat,dlat, slon, dlon, num_flights in source_to_dest:
        fig.add_trace(go.Scattergeo(
                            lat = [slat,dlat],
                            lon = [slon, dlon],
                            mode = 'lines',
                            line = dict(width = 1, color="red"),
                            # disable hover info
                            hoverinfo="skip",
                            textposition="top center"
                    ))

    ## Logic to create labels of source and destination cities of flights
    if scope=="airports":
        cities = df_aggregated["AIRPORT_x"].values.tolist()+df_aggregated["AIRPORT_y"].values.tolist()
    else:
        cities = df_aggregated["State_x"].values.tolist()+df_aggregated["State_y"].values.tolist()

    scatter_hover_data = [city for city in cities]

    if query == "AverageArrivalDelay":
        df_aggregated[query] = df_aggregated[query] + df_aggregated[query].min()*-1

    # create a column as concatenation of AIRPORT_x and query
    target_col = "AIRPORT_y" if scope=="airports" else "State_y"

    df_aggregated[target_col] = df_aggregated[target_col] + "<br>"+query+" : "+ df_aggregated[query].astype(str)
    text = df_aggregated[target_col].values.tolist()

    df_aggregated[query]=df_aggregated[query]/df_aggregated[query].max()
    ## Loop thorugh each flight entry to plot source and destination as points.
    fig.add_trace(
        go.Scattergeo(
                    lon = df_aggregated["DEST_LONGITUDE"].values.tolist(),
                    lat = df_aggregated["DEST_LATITUDE"].values.tolist(),
                    hoverinfo = 'text',
                    text = text,
                    mode = 'markers',
                    marker = dict(size = df_aggregated[query]*20+1, color = 'blue', opacity=0.9)),
                    # define the size of the marker based on the number of flights
                    #     
        )

    ## Update graph layout to improve graph styling.
    fig.update_layout(title_text="Connection Map Depicting Flights from Brazil to All Other Countries",
                      height=700, width=900,
                      margin={"t":0,"b":0,"l":0, "r":0, "pad":0},
                      showlegend=False,
                      geo= dict(showland = True, landcolor = 'white', countrycolor = 'grey', bgcolor="lightgrey",scope='north america'))

    return fig