##### IMPORTS ######

import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from plotly.subplots import make_subplots

from spark_api import compute_flights_per_place, compute_flights_per_selected_place, compute_mean_arr_delay_per_dest, compute_mean_dep_delay_per_origin, compute_x_places_by_interval, get_column_aliases, get_column_per_agg_level, matrix_agg, origin_dest_query, reporting_airlines_queries, routes_queries, \
                    scatter_queries, states_map_query, textual_queries


##### CONSTANTS ######

week_days_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
months_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
states = pd.read_csv("util/states.csv",delimiter="\t",header=None)
states.columns = ["State","unk","Abbreviation"]
airports = pd.read_csv("util/airports.csv")
airlines = pd.read_csv("util/airlines.csv")
cancellations = pd.read_csv("util/cancellations.csv")

column_aliases = get_column_aliases()
column_per_aggregation_level = get_column_per_agg_level()

##### FUNCTIONS #######


# heatmap plot

def matrix_plot(df,x,y,z="count"):
    df_pd = matrix_agg(df,x,y,z).toPandas()

    fig = px.imshow(
        df_pd.pivot(y, x, f"{z}_agg"), 
        labels=dict(x=x, y=y, color=f"{z}_agg"),
        y=week_days_names,
        x= months_names if x=="Month" else df_pd[x].unique().sort(),
    )
    return fig

# pie plot

def origin_dest_plot(df,from_date,to_date,query="ArrDelay"):
    df_pd = origin_dest_query(df,from_date,to_date,query).toPandas()
    # complete with full name for states (e.g. CA -> California)
    df_pd = df_pd.merge(states, left_on="ORIGIN_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Origin"}).\
        merge(states, left_on="DEST_STATE", right_on="Abbreviation").\
        rename(columns={"State": "Dest"})
    # create a new column with origin and destination as a single element
    df_pd["Origin-Dest"] = df_pd["Origin"] + " - " + df_pd["Dest"]
    # create title based on query
    title = "Arrival delay by origin and destination" if query=="ArrDelay" else "Number of flights by origin and destination"
    title+=" from "+str(from_date.strftime("%d-%m-%Y"))+" to "+str(to_date.strftime("%d-%m-%Y"))
    fig = px.pie(df_pd.head(20), values=query, names='Origin-Dest', title=title)
    return fig


# map routes plot

def plot_routes(df,date_start,date_to,origin="BOS",query="NumFlights",scope="airports"):
    df_aggregated=routes_queries(df,date_start,date_to,origin,query,scope).toPandas()
    # complete IATA for airports with full name
    if scope == "airports":
        df_aggregated = df_aggregated.merge(airports, left_on="Origin", right_on="IATA")
        df_aggregated = df_aggregated.merge(airports, left_on="Dest", right_on="IATA")
    # states abbreviation to full name
    else:
        df_aggregated = df_aggregated.merge(states, left_on="ORIGIN_STATE", right_on="Abbreviation")
        df_aggregated = df_aggregated.merge(states, left_on="DEST_STATE", right_on="Abbreviation")
        
    
    fig = go.Figure()
    
    # create tuple with latitudes of source and destination and query type
    source_to_dest = zip(df_aggregated["ORIGIN_LATITUDE"], df_aggregated["DEST_LATITUDE"],
                         df_aggregated["ORIGIN_LONGITUDE"], df_aggregated["DEST_LONGITUDE"],
                         df_aggregated[query])

    # loop thorugh each flight entry to add line between source and destination
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

    # Logic to create labels of source and destination cities of flights
    if scope=="airports":
        cities = df_aggregated["AIRPORT_x"].values.tolist()+df_aggregated["AIRPORT_y"].values.tolist()
    else:
        cities = df_aggregated["State_x"].values.tolist()+df_aggregated["State_y"].values.tolist()

    #scatter_hover_data = [city for city in cities]

    if query == "AverageArrivalDelay":
        df_aggregated[query] = df_aggregated[query] + df_aggregated[query].min()*-1

    # create a column as concatenation of AIRPORT_x and query
    target_col = "AIRPORT_y" if scope=="airports" else "State_y"

    # create the text to show when hovering with mouse
    df_aggregated[target_col] = df_aggregated[target_col] + "<br>"+query+" : "+ df_aggregated[query].astype(str)
    text = df_aggregated[target_col].values.tolist()

    # define the size of the marker based on value of query
    df_aggregated[query]=df_aggregated[query]/df_aggregated[query].max()
    # subsitute the nan values with 0 if any
    df_aggregated[query] = df_aggregated[query].fillna(0)
    
    # Loop thorugh each flight entry to plot source and destination as points.
    fig.add_trace(
        go.Scattergeo(
                    lon = df_aggregated["DEST_LONGITUDE"].values.tolist(),
                    lat = df_aggregated["DEST_LATITUDE"].values.tolist(),
                    hoverinfo = 'text',
                    text = text,
                    mode = 'markers',
                    marker = dict(size = df_aggregated[query]*20+1, color = 'blue', opacity=0.9))
        )

    # Update graph layout to improve graph styling.
    fig.update_layout(title_text="",
                      #height=700, width=900,
                      margin={"t":0,"b":0,"l":0, "r":0, "pad":0},
                      showlegend=False,
                      geo= dict(showland = True, 
                                landcolor = 'white', 
                                countrycolor = 'grey', 
                                bgcolor="lightgrey",
                                scope='north america'))

    # put the plot in the center of the page
    fig.update_layout(
        autosize=False,
        width=1200,
        height=700,
        margin=dict(
            l=0,
            r=0,
            b=0,
            t=0,
            pad=0
        ),
    )

    return fig

# map states plot

def plot_states_map(df,group,query):
    df_avg = states_map_query(df,group).toPandas()

    # remove AS and GU as they are not in the map. They don't appear in the map
    df_avg = df_avg.drop(df_avg[df_avg[group] == "AS"].index)
    df_avg = df_avg.drop(df_avg[df_avg[group] == "GU"].index)
    
    # join with states for abbreviation conversion
    df_avg = df_avg.merge(states, left_on=group, right_on="Abbreviation")

    fig = px.choropleth(df_avg,locations=group, 
                        locationmode="USA-states", 
                        color=query, scope="usa",
                        hover_data=["State"])

    title = "Average arrival delay " if query=="ArrDelay" else "Number of flights "
    title+="by origin state " if group=="ORIGIN_STATE" else "by destination state "
    fig.update_layout(title_text=title)
    
    return fig


# airline plot

def plot_reporting_airlines(df,from_date,to_date,query="count"):
    if query == "Cancelled":
        df_agg = cancellations.merge(airlines,left_on="Reporting_Airline",right_on="IATA")
        df_agg = df_agg.sort_values(by="Cancelled",ascending=False)
    else:
        df_agg = reporting_airlines_queries(df,from_date,to_date,query).toPandas()
        df_agg = df_agg.merge(airlines, left_on="Reporting_Airline", right_on="IATA")

    if query=="count":
        title = "Number of flights by reporting airline"
    elif query=="avg":
        title = "Average arrival delay by reporting airline"
    else:
        title = "Number of cancelled flights by reporting airline"
    if query == "avg":
        y = "ArrDelay"
    elif query == "count":
        y = "count"
    else:
        y = "Cancelled"
    
    fig = px.bar(df_agg, x="Name", y=y, color="Name")
    fig.update_layout(title_text=title)

    return fig

# scatter plot

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

# textual "plot", it makes a simple redirection to the textual_queries function from
# spark api

def plot_textual(df,date_from, date_to):
    return textual_queries(df,date_from,date_to)


def plot_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by):
    places = compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by)
    place_column_alias = column_aliases[place_attribute]
    title = str(sort_by) + " " + str(x) + " " + place_column_alias
    x_places_hist_plot = px.histogram(places.toPandas(), x=place_attribute, y="Count", color=px.colors.qualitative.Vivid[0:x], title=title,
                                   labels = {
                                            place_attribute: place_column_alias,
                                            "sum of Count": "Count"})
    x_places_hist_plot.update_layout(showlegend=False) 
    return x_places_hist_plot

def pie_plot_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by):
    places = compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by)
    place_column_alias = column_aliases[place_attribute]
    title = sort_by + " " + str(x) + " " + place_column_alias 
    flights_pie_plot = px.pie(places.toPandas(), values='Count', names=place_attribute, title=title,
                              labels = { 
                                        place_attribute: place_column_alias,
                                        "sum of Count": "Count"})
    return flights_pie_plot

# si possono eseguirei in parallelo le prime due query 
def facet_plot_over_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by):
    places = compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by)
    places = np.array(places.select(place_attribute).collect()).reshape(-1)
    flights_per_place  = compute_flights_per_place(flights_df, start_month, end_month, start_day, end_day, place_attribute)
    fig = make_subplots(rows=x, cols=1) 
    flights_per_place = flights_per_place.toPandas()
    for i in range(len(places)):
        place = places[i]
        flights_per_place_i_pd = flights_per_place[flights_per_place[place_attribute] == place]
        place_column_alias = column_aliases[place_attribute]

        fig.add_trace(
            go.Scatter(x=flights_per_place_i_pd['FlightDate'], y=flights_per_place_i_pd['count'], 
                        name=place),
            row=i+1, col=1
        )
        
    title = sort_by + " " + str(x) + " "  + place_column_alias 
    fig.update_layout(height=1100, width=1200, title_text=title)
    return fig


def plot_mean_arr_delay_per_dest(flights_df, destinations, dest_attribute, aggregation_level):
    mean_arr_delay_per_dest = compute_mean_arr_delay_per_dest(flights_df, destinations, dest_attribute, aggregation_level)
    period = column_per_aggregation_level[aggregation_level]
    y = "avg(" + "ArrDelayMinutes" + ")"
    dest_column_alias = column_aliases[dest_attribute]
    mean_arr_delay_plot = px.line(mean_arr_delay_per_dest.toPandas(), x=period, y=y, color=dest_attribute,
                                        labels = {dest_attribute: dest_column_alias,
                                                  "avg(ArrDelayMinutes)": "Average arrival delay (Minutes)"})
    return mean_arr_delay_plot


def plot_mean_dep_delay_per_origin(flights_df, origins, origin_attribute, aggregation_level):
    mean_dep_delay_per_origin = compute_mean_dep_delay_per_origin(flights_df, origins, origin_attribute, aggregation_level)
    period = column_per_aggregation_level[aggregation_level]
    y = "avg(" + "DepDelayMinutes" + ")"
    origin_column_alias = column_aliases[origin_attribute]
    mean_dep_delay_plot = px.line(mean_dep_delay_per_origin.toPandas(), x=period, y=y, color=origin_attribute,
                                        labels = {origin_attribute: origin_column_alias,
                                                  "avg(DepDelayMinutes)": "Average departure delay (Minutes)"})
    return mean_dep_delay_plot

def plot_num_of_flights_facet(flights_df, place, place_column):
    num_of_flights_per_selected_place = compute_flights_per_selected_place(flights_df, place_column, place)
    num_of_flights_facet_plot = px.line(num_of_flights_per_selected_place.toPandas(), x="FlightDate", y="count", 
                                        labels = {"count": "Count"})
    return num_of_flights_facet_plot