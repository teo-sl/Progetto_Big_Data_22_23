
from plots_api import matrix_plot, origin_dest_plot, plot_reporting_airlines, plot_routes, plot_scatter, plot_states_map
from spark_api import get_dates, load_dataset

df = load_dataset()
dates = get_dates()
cache = {}
loading_style = {'position': 'absolute', 'align-self': 'center'}



def update_matrix(x_axis, z_axis, n_clicks):
    new_loading_style = loading_style
    key = 'matrix1'+str(x_axis)+str(z_axis)
    if key in cache:
        ret = cache[key]
    else:
        ret = matrix_plot(df, x_axis, 'DayOfWeek', z_axis)
        cache[key] = ret

    return ret, new_loading_style


def update_pie(z_axis, date_range, n_clicks):
    new_loading_style = loading_style
    key = 'pie1'+str(z_axis)+str(date_range[0])+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = origin_dest_plot(
            df, dates[date_range[0]], dates[date_range[1]], z_axis)
        cache[key] = ret

    return ret, new_loading_style


def update_map(origin, scope, date_range, query, n_clicks):
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

def update_states(orig_dest,query,n_clicks):
    new_loading_style = loading_style
    key = 'state1'+str(orig_dest)+str(query)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_states_map(df,orig_dest,query)
        cache[key] = ret

    return ret, new_loading_style

def update_airlines(query,date_range,n_clicks):
    new_loading_style = loading_style
    key = 'airline1'+str(query)+str(date_range[0])+str(date_range[1])
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_reporting_airlines(df,dates[date_range[0]],dates[date_range[1]],query)
        cache[key] = ret
    return ret, new_loading_style

def update_scatter(time,x,y,z,n_clicks):
    new_loading_style = loading_style
    key = 'scatter1'+str(time)+str(x)+str(y)+str(z)
    if key in cache:
        ret = cache[key]
    else:
        ret = plot_scatter(df,time,x,y,z)
        cache[key]=ret
    return ret,new_loading_style