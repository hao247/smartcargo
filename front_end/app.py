import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import yaml
import plotly.graph_objs as go
import pandas as pd
import psycopg2 as pg


def generate_ports():
    ports =[]
    for key in port_list.keys():
        ports.append({'label': key, 'value': key})
    return ports

def generate_histogram(col, title, x_title, y_title):
    trace = go.Histogram(
        x = col,
        xbins=dict(
            #start=0,
            #end=30,
            size=3,
        ),
        autobinx=False
    )
    layout = go.Layout(
        title = title,
        xaxis = dict(title=x_title),
        yaxis = dict(title=y_title),
    )
    return go.Figure(
        data=[trace],
        layout=layout,
    )


def generate_pie(count, title):
    labels = []
    values = []
    for i in count.items():
        labels.append(i[0])
        values.append(i[1])

    trace = go.Pie(
        labels = labels,
        values = values,
    )
    layout = go.Layout(
        title = title,
    )
    return go.Figure(
        data=[trace],
        layout=layout,
    )

port_list = yaml.load(open('/home/ubuntu/git/smartcargo/config/port_list.yaml', 'r'))
credent = yaml.load(open('/home/ubuntu/git/smartcargo/config/credentials.yaml', 'r'))
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
ports = generate_ports()
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='SmartCargo  (under construction)'),

    #html.Div(children='''
    #    Dash: A web application framework for Python
    #'''),

    html.Div([
        html.Label('Start:'),
        dcc.Dropdown(
            id='port_start',
            options=ports,
            value='Departure port'
        ),
    ],style={'width': '25%', 'display': 'inline-block'}),
    
    html.Div([
        html.Label('End:'),
        dcc.Dropdown(
            id='port_end',
            options=ports,
            value='Destination port'
        )
    ],style={'width': '25%', 'display': 'inline-block'}),

    html.Div(
        id='show-ports'
    ),

    html.Div([
        dcc.Graph(
            id='duration_histogram',
        ),
    ],style={'width': '40%', 'display': 'inline-block'}),
    
    html.Div([
        dcc.Graph(
            id='ship_counts',
        ),
    ],style={'width': '40%', 'display': 'inline-block'}),
])


@app.callback(Output('duration_histogram', 'figure'),
        [Input('port_start', 'value'),
         Input('port_end', 'value')])
def show_duration(port_start, port_end):
    conn = pg.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    trips = pd.read_sql_query("select * from trips_test where departure = '{}' and arrival = '{}'".format(port_start, port_end), con=conn)
    print(trips)
    conn.close()
    return generate_histogram(trips.duration, 'Trip Duration', 'Duration (hours)', 'Count')


@app.callback(Output('ship_counts', 'figure'),
        [Input('port_start', 'value'),
         Input('port_end', 'value')])
def show_(port_start, port_end):
    conn = pg.connect(\
            host=credent['psql']['host'],\
            database=credent['psql']['dbname'],\
            user=credent['psql']['user'],\
            password=credent['psql']['passwd'])
    
    trips = pd.read_sql_query("select * from trips_test where departure = '{}' and arrival = '{}'".format(port_start, port_end), con=conn)
    ships = pd.read_sql_query("select * from ship_info_test", con=conn)
    df = trips.set_index('mmsi').join(ships.set_index('mmsi'))
    
    conn.close()
    return generate_pie(df.cargo.value_counts().to_dict(), 'Ship Type')

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=50000, debug=True)
