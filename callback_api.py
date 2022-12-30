
from plots_api import matrix_plot
from spark_api import load_dataset

df = load_dataset()
cache = {}
loading_style = {'position': 'absolute', 'align-self': 'center'}

def update_graph(x_axis, z_axis, n_clicks):
    new_loading_style = loading_style
    key = 'matrix1'+str(x_axis)+str(z_axis)
    if key in cache:
        ret = cache[key]
    else:
        ret = matrix_plot(df, x_axis, 'DayOfWeek', z_axis)
        cache[key] = ret

    return ret, new_loading_style