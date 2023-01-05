####### IMPORTS #########

import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import pandas as pd
import pickle


from plots_api import matrix_plot, origin_dest_plot, plot_reporting_airlines, \
                      plot_routes, plot_scatter, plot_states_map, plot_textual

from spark_api import get_dates, load_cache, load_dataset


####### LOAD DATA #######

cache = load_cache()
df = load_dataset()
dates = get_dates()
airports = pd.read_csv("util/airports.csv")


####### APP LAYOUT #######

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


###### UTIL FUNCTIONS #########


def get_graph(class_name, **kwargs):
    return html.Div(
        className=class_name + ' plotz-container',
        children=[
            dcc.Graph(**kwargs),
            html.I(className='fa fa-expand'),
        ],
    )

####### APP COMPONENTS #######


# heatmap plot

plot1 = html.Div(
    className='plot-container',
    
    style={'margin-left': '4%', 'margin-right': '4%'},
    
    children=[
        html.H2('Heatmap widget',style={'text-align': 'center'}),
        dbc.Row([
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
            ]
        ),
        dbc.Row(
            get_graph(
                'plot',
                id='matrix',
                config=plot_config,
                figure={}
            ),
        ),
    ]
)


slider = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-2',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%d-%m-%Y")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)

# pie plot, origin-destination

plot2 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Pie widget',style={'text-align': 'center'}),
        dbc.Row([
                 slider,
                 html.Br(),
                 html.H4('Select the Z axis'),
                 dcc.RadioItems(
                     id='query-pie',
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
                             id='button-pie',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='load-pie',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),

            dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='pie-routes',
                        config=plot_config,
                        figure={}
                    ),
                    dcc.Loading(id='loading', parent_style=loading_style)
                ])
            ),
    ]
)



slider_map = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-3',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%d-%m-%Y")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)

# map for routes

plot3 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
            dbc.Row([
                 html.H2('Map of routes by average delay or number of flights',style={'text-align': 'center'}),
                 html.H5('',id='map-routes-title',style={'text-align': 'center'}),
                 html.Br(),
                 slider_map,   
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
                 html.H4('Select the origin'),
                 dcc.Dropdown(
                     id='origin-map-routes',
                     options=[{'label': airports["AIRPORT"][i], 'value': airports["IATA"][i]}
                              for i in range(len(airports["IATA"]))],
                     value='BOS',
                     clearable=False
                 ),
                html.Br(), 
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
                 html.Br(),
            ]),
            dbc.Row(
                    html.Div([
                        html.Br(),
                        get_graph(
                            'plot',
                            id='map-routes',
                            config=plot_config,
                            figure={}
                        ),
                    ])
            ),
    ]
)

# states plot

plot4 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Map of states by average delay or number of flights',style={'text-align': 'center'}),
        dbc.Row([
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
        dbc.Row(
                get_graph(
                    'plot',
                    id='plot-state',
                    config=plot_config,
                    figure={}
                ),
        ),
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
            marks={i: dates[i].strftime("%d-%m-%Y")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)

# airlines plot

plot5 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
            html.H2('Flights per airline',style={'text-align': 'center'}),
            html.H5('',id='airline-period',style={'text-align': 'center'}),
            dbc.Row([
                slider_airlines,
                html.Br(),
                html.H4('Select the type of query'),
                
                dcc.RadioItems(
                    id='query-airlines',
                    options=[
                        {'label': 'Count', 'value': 'count'},
                        {'label': 'Delay', 'value': 'avg'},
                        {'label': 'Cancelled', 'value': 'Cancelled'}
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
            dbc.Row(
                get_graph(
                    'plot',
                    id='plot-airline',
                    config=plot_config,
                    figure={}
                ),
            ),
                
        ])
    ]
)

option_scatter = [{"label" : "Arrival delay", "value" : "ArrDelay"},
                {"label":"Departure delay" ,"value" :"DepDelay"},
                {"label":"Number of flights" ,"value" :"count"},
                {"label": "Taxi in time","value" :"TaxiIn"},
                {"label": "Taxi out time","value" :"TaxiOut"},
                {"label": "Air time","value" :"AirTime"},
                {"label": "Distance","value" :"distance"}
            ]

# scatter plot

plot6 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
            html.H2('Scatter widget',style={'text-align': 'center'}),
            dbc.Row([
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
            dbc.Row(
                get_graph(
                    'plot',
                    id='plot-scatter',
                    config=plot_config,
                    figure={}
                ),
            ),
        ])
    ]
)

slider_text = html.Div(
    className='slider',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='date-slider',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%d-%m-%Y")
                   for i in range(0, len(dates), 60)},
        ),
    ],
)

# numeric data

plot7 = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Some numeric info',style={'text-align': 'center'}),
        html.H5('',id='text-period',style={'text-align': 'center'}),
        dbc.Row([
            slider_text,
            html.Br(),
            html.Div(
                className='control-panel',
                children=[
                    html.Button(
                        'Update',
                        id='button-text',
                        n_clicks=0,
                        style={'flex-grow': '1'}
                    ),
                    dcc.Loading(id='load-text',
                                parent_style=loading_style)
                ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
            ),
        ]),
        html.Br(),
        dbc.Row(
            html.Div(
                className='textual-data',
                style={'font-size': '2.5em'},
                children=[
                    html.Br(),
                    html.P('Number of flights:'),
                    html.P('',
                        style={'color': 'red','text-align':'center'},
                        id='textual-num'
                    ),
                    html.P('Number of cancelled flights:'),
                    html.P('',
                        style={'color': 'orange','text-align':'center'},
                        id='textual-cancelled'
                    ),
                    html.P('Number of delayed flights:'),
                    html.P('',
                        style={'color': 'purple','text-align':'center'},
                        id='textual-delayed'
                    ),
                    html.P('Number of diverted flights:'),
                    html.P('',
                        style={'color': 'pink','text-align':'center'},
                        id='textual-diverted'
                    ),
                    html.P('Average delay:'),
                    html.P('',
                        style={'color': 'brown','text-align':'center'},
                        id='textual-average-delay'
                    ),
                ]
            )
        )
    ]
)

###### APP HEADER ########

about_app = html.Div(
    children=[
        html.H2('About the Dashboard'),
        html.P('''
        Welcome to the 2013 Flight Data Dashboard! This interactive tool allows 
        you to explore and analyze flight data from across the United States for 
        the entire year of 2013. With this dashboard, you can gain insights into 
        flight patterns, delays, and performance, as well as compare data across 
        different airlines and airports. Start by selecting your preferred filters 
        and then dive into the data using the various charts and graphs. 
        Thank you for using our dashboard! \n 
        Authors: \n
        Filippo Andrea Folino \n
        Teodoro Sullazzo
        ''')
    ]
)


modal = html.Div(
    children=[
        dbc.Button(
            'About the Dashboard',
            id='open_modal',
            color='link',
            style={
             'background-color': 'blue',
              'color': 'white',
              'border': '0px',
              'font-weight': 'bold',
              'hover': { 
                     'color': '#ffffff'
              }
            }

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

###### CACHE ########

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


##### COMPONING APP ######

app.layout = html.Div(
    className='flight-container',
    children=[
        html.Header(
            className='header',
            style={'background-color': '#4d004b'},

            children=[
                html.Div(
                    
                    style={'color': 'white', 'margin-left': '10px'},
                    className='title',
                    children=[
                        html.H1('Flight data'),
                        html.H2('Visualizing flight data from the US'),
                    ]
                ),
                html.Div(
                    className='header',
                    
                    style={'display': 'flex', 'justify-content': 'center'},
                    children=[
                        modal,
                        
                    ])
            ]),
        cache_saver,

        html.Hr(style = {'border': '5px solid black'}),

        plot1,

        html.Hr(style = {'border': '5px solid black'}),

        plot2,

        html.Hr(style = {'border': '5px solid black'}),

        plot3,

        html.Hr(style = {'border': '5px solid black'}),

        plot4,

        html.Hr(style = {'border': '5px solid black'}),

        plot5,

        html.Hr(style = {'border': '5px solid black'}),

        plot6,

        html.Hr(style = {'border': '5px solid black'}),

        plot7,
    ])


###### CALLBACKS ########

# cache saving

@app.callback(
    Output('placeholder','children'),
    (Input('chache-timeout','n_intervals')),
)
def cache_save(n_intervals):
    pickle.dump(cache,open('util/cache.pkl','wb'))
    return []


# header button

@app.callback(
    Output('modal', 'is_open'),
    [Input('open_modal', 'n_clicks'), Input('close', 'n_clicks')],
    [State('modal', 'is_open')],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return

# update heatmap

@app.callback(
    [Output('matrix', 'figure'), Output('loading-1', 'parent_style')],
    [State('x-axis-1', 'value'),
        State('z-axis-1', 'value'),
        Input('update-button-1', 'n_clicks')
     ])
def update_graph(x_axis, z_axis, n_clicks):
    new_loading_style = loading_style
    key = 'matrix1 '+str(x_axis)+' '+str(z_axis)
    if key in cache:
        ret = cache[key]
    else:
        ret = matrix_plot(df, x_axis, 'DayOfWeek', z_axis)
        cache[key] = ret

    return ret, new_loading_style

# update pie chart

@app.callback(
    [Output('pie-routes', 'figure'), Output('load-pie', 'parent_style')],
    [State('query-pie', 'value'),
        State('slider-2', 'value'),
        Input('button-pie', 'n_clicks')
     ])
def update_graph(z_axis, date_range, n_clicks):
    new_loading_style = loading_style
    key = 'pie1 '+str(z_axis)+' '+str(date_range[0])+' '+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = origin_dest_plot(
            df, dates[date_range[0]], dates[date_range[1]], z_axis)
        cache[key] = ret

    return ret, new_loading_style

# update map routes

@app.callback(
    [Output('map-routes', 'figure'), Output('load-map-routes', 'parent_style'),Output('map-routes-title','children')],
    [State('origin-map-routes', 'value'),
        State('air-state-map-routes', 'value'),
        State('slider-3', 'value'),
        State('query-map-routes', 'value'),
        Input('button-map-routes', 'n_clicks')
     ])
def update_graph(origin, scope, date_range, query, n_clicks):
    title_text = 'Period: from ' + dates[date_range[0]].strftime('%d-%m-%Y') + ' to ' + dates[date_range[1]].strftime('%d-%m-%Y')
    new_loading_style = loading_style
    key = 'map1 '+str(origin)+' '+str(scope) +' '+\
        str(date_range[0])+' '+str(date_range[1])+' '+str(query)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_routes(df, dates[date_range[0]],
                          dates[date_range[1]], origin, query, scope)
        cache[key] = ret

    return ret, new_loading_style,title_text

# update map states

@app.callback(
    [Output('plot-state','figure'),Output('load-state','parent_style')],
    [State('orig-dest-selector','value'),
    State('query-state','value'),
    Input('button-state','n_clicks')]
)
def update_graph(orig_dest,query,n_clicks):
    new_loading_style = loading_style
    key = 'state2 '+str(orig_dest)+' '+str(query)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_states_map(df,orig_dest,query)
        cache[key] = ret

    return ret, new_loading_style

# update airline reporting

@app.callback(
    [Output('plot-airline','figure'),Output('load-airlines','parent_style'),Output('airline-period','children')],
    [State('query-airlines','value'),
    State('slider-4','value'),
    Input('button-airlines','n_clicks')]
)
def update_graph(query,date_range,n_clicks):
    title_text = 'Period: from ' + dates[date_range[0]].strftime('%d-%m-%Y') + ' to ' + dates[date_range[1]].strftime('%d-%m-%Y')
    new_loading_style = loading_style
    key = 'airline1 '+str(query)+' '+str(date_range[0])+' '+str(date_range[1])        
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_reporting_airlines(df,dates[date_range[0]],dates[date_range[1]],query)
        cache[key] = ret
    return ret, new_loading_style,title_text

# update scatter

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
    key = 'scatter1 '+str(time)+' '+str(x)+' '+str(y)+' '+str(z)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_scatter(df,time,x,y,z)
        cache[key]=ret
    return ret,new_loading_style


# update textual

@app.callback(
    [
        Output('textual-num','children'),
        Output('textual-cancelled','children'),
        Output('textual-delayed','children'),
        Output('textual-diverted','children'),
        Output('textual-average-delay','children'),
        Output('load-text','parent_style'),
        Output('text-period','children')
    ],
    [
        State('date-slider','value'),
        Input('button-text','n_clicks')
    ]
)
def update_text(date_range,n_clics):
    title_text = 'Period: from ' + dates[date_range[0]].strftime('%d-%m-%Y') + ' to ' + dates[date_range[1]].strftime('%d-%m-%Y')
    new_loading_style = loading_style
    key = 'text3 '+str(date_range[0])+' '+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_textual(df,dates[date_range[0]],dates[date_range[1]])
        cache[key]=ret
    ret = ret[:]
    for i in range(len(ret)):
        ret[i] = str(ret[i])
    ret[0] = '{:,}'.format(int(ret[0]))
    ret[1] = '{:,}'.format(int(float(ret[1])))
    ret[2] = '{:,}'.format(int(ret[2]))
    ret[3] = '{:,}'.format(int(float(ret[3])))

    if ret[4].find('.') != -1:
        ret[4] = ret[4][0:ret[4].find('.')+3]    
    ret[4] = ret[4]+' minutes'

    return ret[0],ret[1],ret[2],ret[3],ret[4],new_loading_style,title_text

##### APP RUN #######

# run the app debug mode and 9000 port
if __name__ == '__main__':
    app.run_server(debug=True, port=9000)
