import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plotly.subplots import make_subplots
import plotly.offline as py

from spark_api import matrix_agg, origin_dest_query, reporting_airlines_queries, routes_queries, scatter_queries, states_map_query

week_days_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
months_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
states = pd.read_csv("util/states.csv",delimiter="\t",header=None)
states.columns = ["State","unk","Abbreviation"]
airports = pd.read_csv("util/airports.csv")
airlines = pd.read_csv("util/airlines.csv")


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
    # make a join over STATE_ORIGIN and Abbreviation in states dataframe, rename the columns
    df_pd = df_pd.merge(states, left_on="ORIGIN_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Origin"}).\
        merge(states, left_on="DEST_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Dest"})
    # create a new column with the origin and destination
    df_pd["Origin-Dest"] = df_pd["Origin"] + " - " + df_pd["Dest"]
    title = "Arrival delay by origin and destination" if query=="ArrDelay" else "Number of flights by origin and destination"
    title+=" from "+str(from_date)[0:10]+" to "+str(to_date)[0:10]
    fig = px.pie(df_pd.head(20), values=query, names='Origin-Dest', title=title)
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
    # subsitute the nan values with 0 if any
    df_aggregated[query] = df_aggregated[query].fillna(0)
    
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
    fig.update_layout(title_text="",
                      height=700, width=900,
                      margin={"t":0,"b":0,"l":0, "r":0, "pad":0},
                      showlegend=False,
                      geo= dict(showland = True, landcolor = 'white', countrycolor = 'grey', bgcolor="lightgrey",scope='north america'))

    
    return fig

def plot_states_map(df,group,query):
    df_avg = states_map_query(df,group).toPandas()
    df_avg = df_avg.drop(df_avg[df_avg[group] == "AS"].index)
    df_avg = df_avg.drop(df_avg[df_avg[group] == "GU"].index)
    
    fig = px.choropleth(locations=df_avg[group], locationmode="USA-states", color=df_avg[query], scope="usa")

    title = "Average arrival delay " if query=="AverageArrivalDelay" else "Number of flights "
    title+=" by origin state " if group=="ORIGIN_STATE" else " by destination state "
    fig.update_layout(title_text=title)
    
    return fig


def plot_reporting_airlines(df,from_date,to_date,query="count"):
    df_agg = reporting_airlines_queries(df,from_date,to_date,query).toPandas()
    df_agg = df_agg.merge(airlines, left_on="Reporting_Airline", right_on="IATA")
    title = "Number of flights by reporting airline" if query=="count" else "Average arrival delay by reporting airline"
    # plot the data as a bar chart, colored by the carrier
    y = "ArrDelay" if query == "avg" else "count"
    fig = px.bar(df_agg, x="Name", y=y, color="Name")
    # add a title
    fig.update_layout(title_text=title)
    return fig

def plot_scatter(df,temp_granularity,x,y,z):
    df_pd = scatter_queries(df,temp_granularity).toPandas()
    fig = px.scatter(df_pd, x=x, y=y,color=z,hover_data=[temp_granularity])
    dict = {"ArrDelay" : " the average arrival delay ", 
            "DepDelay" : " the average departure delay ",
            "count" : " the number of flights ",
            "TaxiIn" : " the average taxi in time ",
            "TaxiOut" : " the average taxi out time ",
            "AirTime" : " the average air time ",
            "distance" : " the average distance "
    }
    title = f"Scatter plot with {dict[x]} on the x-axis, {dict[y]} on the y-axis and {dict[z]} as the colormap."
    fig.update_layout(title_text=title)
    return fig