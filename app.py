import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import pandas as pd
import os
import pickle

from plots_api import matrix_plot, origin_dest_plot, plot_reporting_airlines, plot_routes, plot_scatter, plot_states_map

from spark_api import get_dates, load_cache, load_dataset

cache = load_cache()
df = load_dataset()
dates = get_dates()
airports = pd.read_csv("util/airports.csv")


loading_style = {'position': 'absolute', 'align-self': 'center'}

external_stylesheets = [
    'https://codepen.io/mikesmith1611/pen/QOKgpG.css',
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.8.1/css/all.min.css',
    dbc.themes.BOOTSTRAP
]

app = dash.Dash(__name__,
                external_stylesheets=external_stylesheets,
                eager_loading=True,
                )

# use light theme
template = 'plotly_white'
default_layout = {
    'autosize': True,
    'xaxis': {'title': None},
    'yaxis': {'title': None},
    'margin': {'l': 40, 'r': 20, 't': 40, 'b': 10},
    'paper_bgcolor': '#303030',
    'plot_bgcolor': '#303030',
    'hovermode': 'x',
}


plot_config = {
    'modeBarButtonsToRemove': [
        'lasso2d',
        'hoverClosestCartesian',
        'hoverCompareCartesian',
        'toImage',
        'sendDataToCloud',
        'hoverClosestGl2d',
        'hoverClosestPie',
        'toggleHover',
        'resetViews',
        'toggleSpikelines',
        'resetViewMapbox'
    ]
}


def get_graph(class_name, **kwargs):
    return html.Div(
        className=class_name + ' plotz-container',
        children=[
            dcc.Graph(**kwargs),
            html.I(className='fa fa-expand'),
        ],
    )


# create a div screen containing a plot and a control panel
plot1 = html.Div(
    className='plot-container',
    children=[
        dbc.Row(
            [
                dbc.Col(
                    get_graph(
                        'plot',
                        id='matrix',
                        config=plot_config,
                        figure={}#matrix_plot(df, 'Month', 'DayOfWeek', 'count'),
                    ),
                ),
                dbc.Col([
                    html.Div(
                        className='control-panel',
                        children=[
                            html.H4('Select the X axis'),
                            dcc.RadioItems(
                                id='x-axis-1',
                                options=[
                                    {'label': 'Departure time block',
                                        'value': 'DepTimeBlk'},
                                    {'label': 'Month', 'value': 'Month'},
                                ],
                                value='Month',
                            ),

                            html.Br(),

                            html.H4('Select the Z axis'),
                            dcc.RadioItems(
                                id='z-axis-1',
                                options=[
                                    {'label': 'Count', 'value': 'count'},
                                    {'label': 'Delay', 'value': 'ArrDelay'},
                                ],
                                value='count',

                            ),
                        ],
                    ),
                    html.Br(),
                    html.Div(
                        className='control-panel',
                        children=[
                            html.Button(
                                'Update',
                                id='update-button-1',
                                n_clicks=0,
                                style={'flex-grow': '1'}
                            ),
                            dcc.Loading(id='loading-1',
                                        parent_style=loading_style)
                        ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                    ),
                ]),
            ]
        )
    ]
)


# create a slider to select the date range
slider = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-2',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%Y-%m-%d")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)


# create a div screen containing a plot and the slider
plot2 = html.Div(
    className='plot-container',
    children=[
        # create a dbc.row with a plot and the slider
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        get_graph(
                            'plot',
                            id='pie-routes',
                            config=plot_config,
                            figure={}#origin_dest_plot(df, dates[0], dates[100], 'count'),
                            # layout=default_layout,
                        ),
                        dcc.Loading(id='loading', parent_style=loading_style)
                    ])
                ),
                dbc.Col([
                         slider,
                         html.Br(),
                         # create a radio button to select count or delay
                         html.H4('Select the Z axis'),
                         dcc.RadioItems(
                             id='query-pie',
                             options=[
                                 {'label': 'Count', 'value': 'count'},
                                 {'label': 'Delay', 'value': 'ArrDelay'},
                             ],
                             value='count',
                         ),
                         #  add a button to activate the query
                         html.Br(),

                         html.Div(
                             className='control-panel',
                             children=[
                                 html.Button(
                                     'Update',
                                     id='button-pie',
                                     n_clicks=0,
                                     style={'flex-grow': '1'}
                                 ),
                                 dcc.Loading(id='load-pie',
                                             parent_style=loading_style)
                             ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                         ),
                         ]),
            ]
        )
    ]
)

# create a slider named "slider_map" over the dates
slider_map = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-3',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%Y-%m-%d")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)

plot3 = html.Div(
    className='plot-container',
    children=[
        # create a dbc.row with a plot and the slider
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        get_graph(
                            'plot',
                            id='map-routes',
                            config=plot_config,
                            figure={}#plot_routes(df, dates[0], dates[len(dates)-1], origin="BOS", query="NumFlights", scope='airports'),
                            # layout=default_layout,
                        ),
                    ])
                ),
                dbc.Col([
                         html.H2('Map of routes by average delay or number of flights'),
                         html.Br(),
                         slider_map,
                         # create a radio button to select count or delay
                         html.H4('Select the Z axis'),
                         dcc.RadioItems(
                             id='query-map-routes',
                             options=[
                                 {'label': 'Count', 'value': 'NumFlights'},
                                 {'label': 'Delay', 'value': 'AverageArrivalDelay'},
                             ],
                             value='NumFlights',
                         ),
                         html.Br(),
                         # add a dropdown to select the origin from airports
                         html.H4('Select the origin'),
                         dcc.Dropdown(
                             id='origin-map-routes',
                             options=[{'label': airports["AIRPORT"][i], 'value': airports["IATA"][i]}
                                      for i in range(len(airports["IATA"]))],
                             value='BOS',
                         ),
                        html.Br(),
                         # create a radio button to select the scope from airports or states
                         html.H4('Select the scope'),
                         dcc.RadioItems(
                             id='air-state-map-routes',
                             options=[
                                 {'label': 'Airports', 'value': 'airports'},
                                 {'label': 'States', 'value': 'states'},
                             ],
                             value='airports',
                         ),
                         html.Br(),

                         #  add a button to activate the query
                         html.Div(
                             className='control-panel',
                             children=[
                                 html.Button(
                                     'Update',
                                     id='button-map-routes',
                                     n_clicks=0,
                                     style={'flex-grow': '1'}
                                 ),
                                 dcc.Loading(id='load-map-routes',
                                             parent_style=loading_style)
                             ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                         ),
                         ]),
            ]
        )
    ]
)

plot4 = html.Div(
    className='plot-container',
    children=[
        dbc.Row([
            dbc.Col(
                get_graph(
                    'plot',
                    id='plot-state',
                    config=plot_config,
                    figure={}#lot_states_map(df, "ORIGIN_STATE", "count"),
                    # layout=default_layout,
                ),
            ),
            dbc.Col([
                html.H4('Select from origin or destination'),
                dcc.RadioItems(
                    id='orig-dest-selector',
                    options=[
                        {'label': 'Origin', 'value': 'ORIGIN_STATE'},
                        {'label': 'Destination', 'value': 'DEST_STATE'},
                    ],
                    value='ORIGIN_STATE',
                ),
                html.Br(),
                html.H4('Select the type of query'),
                dcc.RadioItems(
                    id='query-state',
                    options=[
                        {'label': 'Count', 'value': 'count'},
                        {'label': 'Delay', 'value': 'ArrDelay'},
                    ],
                    value='count',
                ),
                html.Br(),
                html.Div(
                    className='control-panel',
                    children=[
                        html.Button(
                            'Update',
                            id='button-state',
                            n_clicks=0,
                            style={'flex-grow': '1'}
                        ),
                        dcc.Loading(id='load-state',
                                    parent_style=loading_style)
                    ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                ),
            ]),
        ])
    ]
)

slider_airlines = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-4',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%Y-%m-%d")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)


plot5 = html.Div(
    className='plot-container',
    children=[
        dbc.Row([
            dbc.Col(
                get_graph(
                    'plot',
                    id='plot-airline',
                    config=plot_config,
                    figure={}#plot_reporting_airlines(df,dates[0],dates[len(dates)-1],"count"),
                    # layout=default_layout,
                ),
            ),
            dbc.Col([
                slider_airlines,
                html.Br(),
                html.H4('Select the type of query'),
                
                dcc.RadioItems(
                    id='query-airlines',
                    options=[
                        {'label': 'Count', 'value': 'count'},
                        {'label': 'Delay', 'value': 'avg'},
                    ],
                    value='count',
                ),
                html.Br(),
                html.Div(
                    className='control-panel',
                    children=[
                        html.Button(
                            'Update',
                            id='button-airlines',
                            n_clicks=0,
                            style={'flex-grow': '1'}
                        ),
                        dcc.Loading(id='load-airlines',
                                    parent_style=loading_style)
                    ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                ),
            ]),
        ])
    ]
)

header = html.Header(
    className='header',
    children=[
        html.Div(
            className='header-title',
            children=[
                html.H1('Flight data'),
                html.H2('Visualizing flight data from the US'),
            ],
        ),
        html.Img(
            className='header-logo',
            src=app.get_asset_url('./dash-logo.png'),
        ),
    ],
)

option_scatter = [{"label" : "Arrival delay", "value" : "ArrDelay"},
                {"label":"Departure delay" ,"value" :"DepDelay"},
                {"label":"Number of flights" ,"value" :"count"},
                {"label": "Taxi in time","value" :"TaxiIn"},
                {"label": "Taxi out time","value" :"TaxiOut"},
                {"label": "Air time","value" :"AirTime"},
                {"label": "Distance","value" :"distance"}
            ]

plot6 = html.Div(
    className='plot-container',
    children=[
        dbc.Row([
            dbc.Col(
                get_graph(
                    'plot',
                    id='plot-scatter',
                    config=plot_config,
                    figure={}#plot_scatter(df,"FlightDate","ArrDelay","DepDelay","count"),
                    # layout=default_layout,
                ),
            ),
            dbc.Col([
                html.H4('Select the time granularity'),
                dcc.RadioItems(
                    id='radio-scatter',
                    options=[
                        {'label':'By day','value':'FlightDate'},
                        {'label':'By month','value':'Month'},
                        {'label':'By day of week','value':'DayOfWeek'}
                    ],
                    value = 'FlightDate'
                ),

                html.Br(),
                
                html.H4('Select the x axis'),
                dcc.Dropdown(
                    id = 'x-scatter',
                    options = option_scatter,
                    value="ArrDelay",
                    clearable=False
                ),
                html.Br(),
                html.H4('Select the y axis'),
                dcc.Dropdown(
                    id = 'y-scatter',
                    options = option_scatter,
                    value="DepDelay",
                    clearable=False
                ),
                html.Br(),
                html.H4('Select the z axis'),
                dcc.Dropdown(
                    id = 'z-scatter',
                    options = option_scatter,
                    value="count",
                    clearable=False
                ),
                html.Br(),
                html.Div(
                    className='control-panel',
                    children=[
                        html.Button(
                            'Update',
                            id='button-scatter',
                            n_clicks=0,
                            style={'flex-grow': '1'}
                        ),
                        dcc.Loading(id='load-scatter',
                                    parent_style=loading_style)
                    ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                ),
            ]),
        ])
    ]
)

header = html.Header(
    className='header',
    children=[
        html.Div(
            className='header-title',
            children=[
                html.H1('Flight data'),
                html.H2('Visualizing flight data from the US'),
            ],
        ),
        html.Img(
            className='header-logo',
            src=app.get_asset_url('./dash-logo.png'),
        ),
    ],
)



about_app = html.Div(
    children=[
        html.H2('About the Dashboard'),
        html.P('''
        lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
        ''')
    ]
)


modal = html.Div(
    [
        dbc.Button(
            'About the Dashboard',
            id='open_modal',
            color='link'
        ),
        dbc.Modal(
            [
                dbc.ModalHeader('US Flight Data'),
                dbc.ModalBody(
                    children=[
                        about_app,
                    ]
                ),
                dbc.ModalFooter(
                    dbc.Button('Close',
                               id='close',
                               color='link',
                               className='ml-auto')
                ),
            ],
            id='modal',
        ),
    ]
)

cache_saver = html.Div(
    id='cache-saver',
    style={'display': 'none'},
    children=[
        dcc.Interval(id='chache-timeout',interval=10*1000,n_intervals=0),
        html.Div(
            id='placeholder',
            children=[]
        )
    ]
)


app.layout = html.Div(
    className='flight-container',
    children=[
        html.Div(
            className='header',
            children=[
                html.Div(
                    className='title',
                    children=[
                        html.H4('Flight data'),
                    ]
                ),
                html.Div(
                    className='header',
                    children=[
                        modal,
                        # control_panel
                    ])
            ]),
        cache_saver,

        plot1,
        plot2,
        plot3,
        plot4,
        plot5,
        plot6,
    ])

@app.callback(
    Output('placeholder','children'),
    (Input('chache-timeout','n_intervals')),
)
def cache_save(n_intervals):
    os.remove('util/cache.pkl')
    pickle.dump(cache,open('util/cache.pkl','wb'))
    return []

@app.callback(
    Output('modal', 'is_open'),
    [Input('open_modal', 'n_clicks'), Input('close', 'n_clicks')],
    [State('modal', 'is_open')],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return

# update the plot when the user selects a different x-axis and z-axis


@app.callback(
    [Output('matrix', 'figure'), Output('loading-1', 'parent_style')],
    [State('x-axis-1', 'value'),
        State('z-axis-1', 'value'),
        Input('update-button-1', 'n_clicks')
     ])
def update_graph(x_axis, z_axis, n_clicks):
    new_loading_style = loading_style
    key = 'matrix1'+str(x_axis)+str(z_axis)
    if key in cache:
        ret = cache[key]
    else:
        ret = matrix_plot(df, x_axis, 'DayOfWeek', z_axis)
        cache[key] = ret

    return ret, new_loading_style

# update the plot when the user selects a different z-axis and time range


@app.callback(
    [Output('pie-routes', 'figure'), Output('load-pie', 'parent_style')],
    [State('query-pie', 'value'),
        State('slider-2', 'value'),
        Input('button-pie', 'n_clicks')
     ])
def update_graph(z_axis, date_range, n_clicks):
    new_loading_style = loading_style
    key = 'pie1'+str(z_axis)+str(date_range[0])+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = origin_dest_plot(
            df, dates[date_range[0]], dates[date_range[1]], z_axis)
        cache[key] = ret

    return ret, new_loading_style

# update the plot when the user selects a different origin and scope


@app.callback(
    [Output('map-routes', 'figure'), Output('load-map-routes', 'parent_style')],
    [State('origin-map-routes', 'value'),
        State('air-state-map-routes', 'value'),
        State('slider-3', 'value'),
        State('query-map-routes', 'value'),
        Input('button-map-routes', 'n_clicks')
     ])
def update_graph(origin, scope, date_range, query, n_clicks):
    new_loading_style = loading_style
    key = 'map1'+str(origin)+str(scope) + \
        str(date_range[0])+str(date_range[1])+str(query)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_routes(df, dates[date_range[0]],
                          dates[date_range[1]], origin, query, scope)
        cache[key] = ret

    return ret, new_loading_style

# update the plot-state when orig-dest-selector and query-state are changed, based on button-state
@app.callback(
    [Output('plot-state','figure'),Output('load-state','parent_style')],
    [State('orig-dest-selector','value'),
    State('query-state','value'),
    Input('button-state','n_clicks')]
)
def update_graph(orig_dest,query,n_clicks):
    new_loading_style = loading_style
    key = 'state1'+str(orig_dest)+str(query)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_states_map(df,orig_dest,query)
        cache[key] = ret

    return ret, new_loading_style

@app.callback(
    [Output('plot-airline','figure'),Output('load-airlines','parent_style')],
    [State('query-airlines','value'),
    State('slider-4','value'),
    Input('button-airlines','n_clicks')]
)
def update_graph(query,date_range,n_clicks):
    new_loading_style = loading_style
    key = 'airline1'+str(query)+str(date_range[0])+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_reporting_airlines(df,dates[date_range[0]],dates[date_range[1]],query)
        cache[key] = ret
    return ret, new_loading_style

@app.callback(
    [Output('plot-scatter','figure'),Output('load-scatter','parent_style')],
    [
        State('radio-scatter','value'),
        State('x-scatter','value'),
        State('y-scatter','value'),
        State('z-scatter','value'),
        Input('button-scatter','n_clicks')
    ]
)
def update_graph(time,x,y,z,n_clicks):
    new_loading_style = loading_style
    key = 'scatter1'+str(time)+str(x)+str(y)+str(z)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_scatter(df,time,x,y,z)
        cache[key]=ret
    return ret,new_loading_style


# run the app debug mode and 9000 port
if __name__ == '__main__':
    app.run_server(debug=True, port=9000)
