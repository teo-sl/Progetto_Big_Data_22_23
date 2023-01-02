import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State

from spark_api import load_dataset, get_column_aliases, get_column_alias_key, get_destinations, get_origins
from plots_api import pie_plot_by_interval, plot_x_places_by_interval, facet_plot_over_interval, plot_mean_arr_delay_per_dest, plot_mean_dep_delay_per_origin,\
                        plot_delay_groups

df = load_dataset()

column_aliases_values = get_column_aliases().values()

days = [1 for i in range(31)]
months = {1:"Jan", 2:"Feb", 3:"March", 4:"April", 5:"May", 6:"June", 7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}

origins_states = get_origins(df, "ORIGIN_STATE")
origins_airports = get_origins(df, "Origin")
destinations_states = get_destinations(df, "DEST_STATE")
destinations_airports = get_destinations(df, "Dest")

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


# create a slider to select the date range
slider_pie = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-1-pie',
            min=1,
            max=len(days),
            value=[1, len(days)],
        ),
        dcc.RangeSlider(
            id='slider-2-pie',
            min=1,
            max=len(months),
            marks=months
        ),
        
    ],
)

slider_x_places = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-1-x-places',
            min=1,
            max=len(days),
            value=[1, len(days)],
        ),
        dcc.RangeSlider(
            id='slider-2-x-places',
            min=1,
            max=len(months),
            marks=months
        ),
        
    ],
)

slider_facet_plot_x_places = html.Div(
    className='slider-container',
    children=[
        html.H4('Select a date range'),
        dcc.RangeSlider(
            id='slider-1-facet-x-places',
            min=1,
            max=len(days),
            value=[1, len(days)],
        ),
        dcc.RangeSlider(
            id='slider-2-facet-x-places',
            min=1,
            max=len(months),
            marks=months
        ),
    ],
)

pie_plot = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Pie widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Top', 'Bottom'],
                    id='sort-by-dropdown-pie',
                    value="Top"
                ))
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ["Destination state", "Destination airport", "Origin state", "Origin airport"],
                    id='selected-place-column-type-by-pie',
                    value="Destination state"
                ))
        ]),
        html.Br(),
        dbc.Row([
                 slider_pie,
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
                         dcc.Loading(id='loading-pie',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
        dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='pie-plot',
                        config=plot_config,
                        figure={}#origin_dest_plot(df, dates[0], dates[100], 'count'),
                        # layout=default_layout,
                    ),
                ])
        ),
    ]
)

hist_plot_x_places = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Top/bottom x places widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Top', 'Bottom'],
                    id='sort-by-dropdown-x-places',
                    value="Top"
                ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ["Destination state", "Destination airport", "Origin state", "Origin airport"],
                    id='selected-place-column-type-hist',
                    value="Destination state"
                ))
        ]),
        html.Br(),
        dbc.Row([
                 slider_x_places,
                 html.Br(),
                 html.Div(
                     className='control-panel',
                     children=[
                         html.Button(
                             'Update',
                             id='button-hist-x',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='loading-hist-x',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
        dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='hist-x-plot',
                        config=plot_config,
                        figure={}
                    ),
                ])
        ),
    ]
)

facet_plot_x_places = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Facet plot of top/bottom x places widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Top', 'Bottom'],
                    id='sort-by-dropdown-facet-x-places',
                    value="Top"
                ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ["Destination state", "Destination airport", "Origin state", "Origin airport"],
                    id='selected-place-column-type-facet',
                    value="Destination state"
                ))
        ]),
        html.Br(),
        dbc.Row([
                 slider_facet_plot_x_places,
                 html.Br(),
                 html.Div(
                     className='control-panel',
                     children=[
                         html.Button(
                             'Update',
                             id='button-facet-x-places',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='loading-facet-x',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
            dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='facet-x-plot',
                        config=plot_config,
                        figure={}
                    ),
                ])
            ),
    ]
)

mean_arr_del_per_dest_plot = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Mean arrival delay per destination widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.RadioItems(
                        id='aggregation-level-arr-delay',
                        options=[
                            {'label': 'Daily', 'value': 'Daily'},
                            {'label': 'Weekly', 'value': 'Weekly'},
                            {'label': 'Monthly', 'value': 'Monthly'},
                        ],
                        value="Daily"
                    ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Destination state', 'Destination airport'],
                    id='selected-dest-type-arr-del',
                    value="Destination state"
                )
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    destinations_states,
                    id='selected-dest-arr-del',
                )
            )
        ]),
        html.Br(),
        dbc.Row([
                 html.Div(
                     className='control-panel',
                     children=[
                         html.Button(
                             'Update',
                             id='button-arr-delay',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='loading-arr-delay',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
            dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='arr-delay-plot',
                        config=plot_config,
                        figure={}
                    ),
    
                ])
            ),
    ]
)

mean_dep_del_per_origin_plot = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Mean departure delay per origin widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.RadioItems(
                        id='aggregation-level-dep-delay',
                        options=[
                            {'label': 'Daily', 'value': 'Daily'},
                            {'label': 'Weekly', 'value': 'Weekly'},
                            {'label': 'Monthly', 'value': 'Monthly'},
                        ],
                        value="Daily"
                    ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Origin state', 'Origin airport'],
                    id='selected-origin-type-dep-del',
                    value="Origin state"
                ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    origins_states,
                    id='selected-origin-dep-del',
                )
            )
        ]),
        html.Br(),
        dbc.Row([
                 html.Div(
                     className='control-panel',
                     children=[
                         html.Button(
                             'Update',
                             id='button-dep-delay',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='loading-dep-delay',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
            dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='dep-delay-plot',
                        config=plot_config,
                        figure={}
                    ),
    
                ])
            ),
    ]
)

delay_groups_plot = html.Div(
    className='plot-container',
    style={'margin-left': '4%', 'margin-right': '4%'},
    children=[
        html.H2('Delay groups widget',style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.RadioItems(
                        id='aggregation-level-dest-delay-group',
                        options=[
                            {'label': 'Daily', 'value': 'Daily'},
                            {'label': 'Weekly', 'value': 'Weekly'},
                            {'label': 'Monthly', 'value': 'Monthly'},
                        ],
                        value="Daily"
                    ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    ['Destination state', 'Destination airport'],
                    id='selected-dest-type-del-group',
                    value="Destination state"
                ),
            )
        ]),
        html.Br(),
        dbc.Row([
            html.Div(
                dcc.Dropdown(
                    destinations_states,
                    id='selected-dest-del-group',
                )
            )
        ]),
        html.Br(),
        dbc.Row([
                 html.Div(
                     className='control-panel',
                     children=[
                         html.Button(
                             'Update',
                             id='button-dest-delay-group',
                             n_clicks=0,
                             style={'flex-grow': '1'}
                         ),
                         dcc.Loading(id='loading-dest-delay-group',
                                     parent_style=loading_style)
                     ], style={'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                 ),
            ]),
        html.Br(),
            dbc.Row(
                html.Div([
                    get_graph(
                        'plot',
                        id='dest-delay-group-plot',
                        config=plot_config,
                        figure={}
                    ),
    
                ])
            ),
    ]
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
    # put in the center of the page
    children=[
        dbc.Button(
            'About the Dashboard',
            id='open_modal',
            # make the button white
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

app.layout = html.Div(
    className='flight-container',
    children=[
        html.Header(
            className='header',
            # make the header purple
            style={'background-color': '#4d004b'},

            children=[
                html.Div(
                    # make the title white and add margin
                    style={'color': 'white', 'margin-left': '10px'},
                    className='title',
                    children=[
                        html.H1('Flight data'),
                        html.H2('Visualizing flight data from the US'),
                    ]
                ),
                html.Div(
                    className='header',
                    # put the children in the center of the header
                    style={'display': 'flex', 'justify-content': 'center'},
                    children=[
                        modal,
                        # control_panel
                    ])
            ]),

        # add a black line to separate the components
        html.Hr(style = {'border': '5px solid black'}),
        pie_plot,
        hist_plot_x_places,
        facet_plot_x_places,
        mean_arr_del_per_dest_plot,
        mean_dep_del_per_origin_plot,
        delay_groups_plot
    ])

@app.callback(
    [Output('pie-plot', 'figure'), Output('loading-pie', 'parent_style')],
    [State('slider-1-pie', 'value'),
        State('slider-2-pie', 'value'),
        State('selected-place-column-type-by-pie', 'value'),
        State('sort-by-dropdown-pie', 'value'),
        Input('button-pie', 'n_clicks')
     ])
def update_graph(selected_day, selected_month, selected_place_column, sort_by, n_clicks):
    start_day = selected_day[0]
    end_day = selected_day[1]
    start_month, end_month = 1, 12
    if (selected_month is not None):
        start_month = selected_month[0]
        end_month = selected_month[1]
    new_loading_style = loading_style
    column_real_name = get_column_alias_key(selected_place_column)
    ret = pie_plot_by_interval(df, 10, start_month, end_month, start_day, end_day, column_real_name, sort_by)
    return ret, new_loading_style

@app.callback(
    [Output('hist-x-plot', 'figure'), Output('loading-hist-x', 'parent_style')],
    [State('slider-1-x-places', 'value'),
        State('slider-2-x-places', 'value'),
        State('selected-place-column-type-hist', 'value'),
        State('sort-by-dropdown-x-places', 'value'),
        Input('button-hist-x', 'n_clicks')
     ])
def update_graph(selected_day, selected_month, selected_place_column, sort_by, n_clicks):
    start_day = selected_day[0]
    end_day = selected_day[1]
    start_month, end_month = 1, 12
    if (selected_month is not None):
        start_month = selected_month[0]
        end_month = selected_month[1]
    new_loading_style = loading_style
    column_real_name = get_column_alias_key(selected_place_column)
    ret = plot_x_places_by_interval(df, 10, start_month, end_month, start_day, end_day, column_real_name, sort_by)
    return ret, new_loading_style

@app.callback(
    [Output('facet-x-plot', 'figure'), Output('loading-facet-x', 'parent_style')],
    [State('slider-1-facet-x-places', 'value'),
        State('slider-2-facet-x-places', 'value'),
        State('selected-place-column-type-facet', 'value'),
        State('sort-by-dropdown-facet-x-places', 'value'),
        Input('button-facet-x-places', 'n_clicks')
     ])
def update_graph(selected_day, selected_month, selected_place_column, sort_by, n_clicks):
    start_day = selected_day[0]
    end_day = selected_day[1]
    start_month, end_month = 1, 12
    if (selected_month is not None):
        start_month = selected_month[0]
        end_month = selected_month[1]
    new_loading_style = loading_style
    column_real_name = get_column_alias_key(selected_place_column)
    ret = facet_plot_over_interval(df, 10, start_month, end_month, start_day, end_day, column_real_name, sort_by)
    return ret, new_loading_style


@app.callback(
    dash.dependencies.Output('selected-dest-arr-del', 'options'),
    dash.dependencies.Input('selected-dest-type-arr-del', 'value')
)
def update_available_dest_dropdown_arr_delay(selected_dest_type):
    return destinations_states if selected_dest_type == "Destination state" else destinations_airports


@app.callback(
    [Output('arr-delay-plot', 'figure'), Output('loading-arr-delay', 'parent_style')],
    [State('aggregation-level-arr-delay', 'value'),
        State('selected-dest-type-arr-del', 'value'),
        State('selected-dest-arr-del', 'value'),
        Input('button-arr-delay', 'n_clicks')
     ])
def update_graph(aggregation_level, dest_type, selected_dest, n_clicks):
    new_loading_style = loading_style
    dest_column_real_name = get_column_alias_key(dest_type)
    ret = plot_mean_arr_delay_per_dest(df, selected_dest,  dest_column_real_name, aggregation_level)
    return ret, new_loading_style

@app.callback(
    dash.dependencies.Output('selected-origin-dep-del', 'options'),
    dash.dependencies.Input('selected-origin-type-dep-del', 'value')
)
def update_available_origins_dropdown(selected_origin_type):
    return origins_states if selected_origin_type == "Origin state" else origins_airports

@app.callback(
    [Output('dep-delay-plot', 'figure'), Output('loading-dep-delay', 'parent_style')],
    [State('aggregation-level-dep-delay', 'value'),
    State('selected-origin-type-dep-del', 'value'),
    State('selected-origin-dep-del', 'value'),
    Input('button-dep-delay', 'n_clicks')
    ])
def update_graph(aggregation_level, origin_type, selected_origin, n_clicks):
    new_loading_style = loading_style
    origin_column_real_name = get_column_alias_key(origin_type)
    ret = plot_mean_dep_delay_per_origin(df, selected_origin, origin_column_real_name, aggregation_level)
    return ret, new_loading_style


@app.callback(
    dash.dependencies.Output('selected-dest-del-group', 'options'),
    dash.dependencies.Input('selected-dest-type-del-group', 'value')
)
def update_available_dest_dropdown_delay_groups(selected_dest_type):
    return destinations_states if selected_dest_type == "Destination state" else destinations_airports

@app.callback(
    [Output('dest-delay-group-plot', 'figure'), Output('loading-dest-delay-group', 'parent_style')],
    [State('aggregation-level-dest-delay-group', 'value'),
    State('selected-dest-type-del-group', 'value'),
    State('selected-dest-del-group', 'value'),
    Input('button-dest-delay-group', 'n_clicks')
    ])
def update_graph(aggregation_level, dest_type, selected_dest, n_clicks):
    new_loading_style = loading_style
    dest_column_real_name = get_column_alias_key(dest_type)
    ret = plot_delay_groups(df, selected_dest, dest_column_real_name, aggregation_level)
    return ret, new_loading_style


# run the app debug mode and 9000 port
if __name__ == '__main__':
    app.run_server(debug=True, port=9000)
