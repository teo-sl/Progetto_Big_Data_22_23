import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from spark_api import compute_x_places_by_interval, compute_flights_per_place, get_column_aliases, get_column_per_agg_level,\
                        compute_mean_arr_delay_per_dest, compute_mean_dep_delay_per_origin, compute_delay_groups, compute_delay_matrix

from plotly.subplots import make_subplots

column_aliases = get_column_aliases()
column_per_aggregation_level = get_column_per_agg_level()


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


def facet_plot_over_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by):
    # top x o bottom x
    places = compute_x_places_by_interval(flights_df, x, start_month, end_month, start_day, end_day, place_attribute, sort_by)
    places = np.array(places.select(place_attribute).collect()).reshape(-1)
    flights_per_place  = compute_flights_per_place(flights_df, start_month, end_month, start_day, end_day, place_attribute)
    fig = make_subplots(rows=x, cols=1) 

    for i in range(len(places)):
        place = places[i]
        flights_per_place_i = flights_per_place.filter(flights_per_place[place_attribute] == place)
        flights_per_place_i_pd = flights_per_place_i.toPandas()
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

def plot_delay_groups(flights_df, destination, dest_attribute, aggregation_level):
    delay_groups = compute_delay_groups(flights_df, destination, dest_attribute, aggregation_level)
    period = column_per_aggregation_level[aggregation_level]
    delay_groups = delay_groups.withColumnRenamed("count", "Count")
    delay_groups_plot = px.bar(delay_groups.toPandas(), x=period, y="Count", color='DepDel15')
    return delay_groups_plot

def plot_scatter_delay_matrix(flights_df, airport_dest):
    delay_matrix = compute_delay_matrix(flights_df, airport_dest)
    delay_matrix_pd = delay_matrix.toPandas()
    matrix_scatter_plot = px.scatter_matrix(delay_matrix_pd, dimensions=["Avg arrival delay", "Avg departure delay"], color="Destination airport")
    matrix_scatter_plot.show()
