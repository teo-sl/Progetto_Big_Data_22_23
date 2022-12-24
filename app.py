import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State

from plots_api import matrix_plot, origin_dest_plot

from spark_api import get_dates, load_dataset

cache={}

df = load_dataset()
dates = get_dates(df)

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
        # create a dbc.row with a plot and a control panel
        dbc.Row(
            [
                dbc.Col(
                    get_graph(
                        'plot',
                        id='matrix',
                        config=plot_config,
                        figure=matrix_plot(df, 'Month', 'DayOfWeek', 'count'),
                        #layout=default_layout,
                    ),
                ),
                dbc.Col([
                    html.Div(
                        className='control-panel',
                        children=[
                            html.H4('Select the X axis'),
                            dcc.RadioItems(
                                id='x-axis',
                                options=[
                                    {'label': 'Departure time block', 'value': 'DepTimeBlk'},
                                    {'label': 'Month', 'value': 'Month'},
                                ],
                                value='Month',
                            ),
                            html.H4('Select the Z axis'),
                            dcc.RadioItems(
                                id='z-axis',
                                options=[
                                    {'label': 'Count', 'value': 'count'},
                                    {'label': 'Delay', 'value': 'ArrDelay'},
                                ],
                                value='count',

                            ),
                        ],
                    ),
                    # add a button to activate the query
                    html.Div(
                        className='control-panel',
                        children=[
                            html.Button(
                                'Update',
                                id='update-plot1',
                                n_clicks=0,
                                style={'flex-grow': '1'}
                            ),
                            dcc.Loading(id='loading1', parent_style=loading_style)
                        ],style= {'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
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
            id='date-range',
            min=0,
            max=len(dates) - 1,
            value=[0, len(dates) - 1],
            marks={i: dates[i].strftime("%Y-%m-%d") for i in range(0, len(dates), 60)},
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
                            id='pie-origin-dest',
                            config=plot_config,
                            figure=origin_dest_plot(df,dates[0],dates[100],'count'),
                            #layout=default_layout,
                        ),
                        dcc.Loading(id='loading', parent_style=loading_style)
                    ])
                ),
                dbc.Col([slider,
                    # create a radio button to select count or delay
                            html.H4('Select the Z axis'),
                            dcc.RadioItems(
                                id='z-axis2',
                                options=[
                                    {'label': 'Count', 'value': 'count'},
                                    {'label': 'Delay', 'value': 'ArrDelay'},
                                ],
                                value='count',
                            ),
                    # add a button to activate the query
                    html.Div(
                        className='control-panel',
                        children=[
                            html.Button(
                                'Update',
                                id='update-plot2',
                                n_clicks=0,
                                style={'flex-grow': '1'}
                            ),
                            dcc.Loading(id='loading2', parent_style=loading_style)
                        ],style= {'position': 'relative', 'display': 'flex', 'justify-content': 'center'}
                    ),
                ]),
            ]
        )
    ]
)


header = html.Div(
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
                        #control_panel
                    ])
            ]),
        plot1,
        plot2,

    ])



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
    [Output('matrix', 'figure'),Output('loading1', 'parent_style')],
    [State('x-axis', 'value'),
        State('z-axis', 'value'),
        Input('update-plot1', 'n_clicks')
    ])
def update_graph(x_axis, z_axis,n_clicks):
    new_loading_style = loading_style
    key = 'matrix1'+str(x_axis)+str(z_axis)
    if key in cache:
        ret = cache[key]
    else:
        ret = matrix_plot(df, x_axis, 'DayOfWeek', z_axis)
        cache[key] = ret

    return ret,new_loading_style

# update the plot when the user selects a different z-axis and time range
@app.callback(
    [Output('pie-origin-dest', 'figure'),Output('loading2', 'parent_style')],
    [State('z-axis2', 'value'),
        State('date-range', 'value'),
        Input('update-plot2', 'n_clicks')
    ])
def update_graph(z_axis, date_range,n_clicks):
    new_loading_style = loading_style
    key = 'pie1'+str(z_axis)+str(date_range[0])+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = origin_dest_plot(df, dates[date_range[0]], dates[date_range[1]], z_axis)
        cache[key] = ret
    
    return ret,new_loading_style
    

# run the app debug mode and 9000 port
if __name__ == '__main__':
    app.run_server(debug=True, port=9000)
