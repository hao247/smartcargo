import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import yaml
import pandas as pd
import psycopg2 as pg
import plots as p


credent = yaml.load(open("../config/credentials.yaml", "r"))
external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# Data Manipulation 

def read_pd_df(query):
    conn = pg.connect(
        host=credent["psql"]["host"],
        database=credent["psql"]["dbname"],
        user=credent["psql"]["user"],
        password=credent["psql"]["passwd"],
    )
    df = pd.read_sql_query(query, con=conn)
    conn.close()
    return df


def generate_ports(port_list):
    ports = []
    for index, port in port_list.iterrows():
        ports.append({"label": port["PORT_NAME"], "value": port["PORT_NAME"]})
    return ports


# Dashboard Layout

port_list = read_pd_df("select * from ports")
ports = generate_ports(port_list)

app.layout = html.Div(
    children=[
        html.Div(
            [
                html.Div(
                    html.Label(
                        ["SmartCargo"],
                        style={
                            "height": 100,
                            "font-family": "Helvetica",
                            "font-weight": "bold",
                            "font-size": "300%",
                            "float": "left",
                            "background-color": "#3FAAB9",
                        },
                    )
                ),
                html.Div(
                    [
                        html.Label(
                            ["Departure port"],
                            style={"color": "orange", "font-size": "150%"},
                        ),
                        dcc.Dropdown(
                            id="port_start", options=ports, value="Departure port"
                        ),
                    ],
                    style={
                        "width": "35%",
                        "display": "inline-block",
                        "font-size": "150%",
                        "margin-bottom": 10,
                    },
                ),
                html.Div(
                    [html.P(children="TO")],
                    style={"display": "inline-block", "fontSize": 30},
                ),
                html.Div(
                    [
                        html.Label(
                            ["Destination port"],
                            style={"color": "red", "font-size": "150%"},
                        ),
                        dcc.Dropdown(
                            id="port_end", options=ports, value="Destination port"
                        ),
                    ],
                    style={
                        "width": "35%",
                        "display": "inline-block",
                        "font-size": "150%",
                        "margin-bottom": 10,
                    },
                ),
            ],
            style={
                "height": 100,
                "text-align": "center",
                "background-color": "#C2E0E4",
            },
        ),
        html.Div(
            [
                html.Div(
                    [html.P(children="Port location")],
                    style={"text-align": "left", "fontSize": 30},
                ),
                dcc.Graph(id="map", figure=p.generate_map(port_list, "null", "null")),
                html.Div(
                    [html.P(children="Port traffic:")],
                    style={"text-align": "left", "fontSize": 30},
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="heat_map_start")],
                            style={
                                "width": "47%",
                                "height": "15%",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [dcc.Graph(id="heat_map_end")],
                            style={
                                "width": "47%",
                                "height": "15%",
                                "display": "inline-block",
                            },
                        ),
                    ]
                ),
            ],
            style={
                "width": "50%",
                "height": "60%",
                "display": "inline-block",
                "vertical-align": "top",
            },
        ),
        html.Div(
            [
                html.Div(
                    [html.P(children="Trip duration:")],
                    style={"text-align": "left", "fontSize": 30},
                ),
                html.Div(
                    [dcc.Graph(id="duration_histogram")],
                    style={"width": "100%", "height": "60%"},
                ),
                html.Div(
                    [html.P(children="Ships in port:")],
                    style={"text-align": "left", "fontSize": 30},
                ),
                html.Div(
                    [dcc.Graph(id="departure_port_traffic")],
                    style={"width": "47%", "display": "inline-block"},
                ),
                html.Div(
                    [dcc.Graph(id="destination_port_traffic")],
                    style={"width": "47%", "display": "inline-block"},
                ),
            ],
            style={
                "width": "50%",
                "height": "60%",
                "display": "inline-block",
                "vertical-align": "top",
            },
        ),
    ],
    style={"text-align": "center", "backgroundColor": "rgb(255,255,255)"},
)


# Dash Callbacks for user interactions

@app.callback(
    Output("duration_histogram", "figure"),
    [Input("port_start", "value"), Input("port_end", "value")],
)
def show_duration(port_start, port_end):
    trips_query = "select * from trips_final_1 where departure = '{}' \
                   and arrival = '{}'".format(
        port_start, port_end
    )
    trips = read_pd_df(trips_query)
    return p.generate_histogram(
        trips.duration, "Trip Duration", "Duration (hours)", "Count"
    )


@app.callback(
    Output("departure_port_traffic", "figure"), [Input("port_start", "value")]
)
def show_departure_port_counts(port_start):
    outbound_query = "select * from trips_final_1 where departure = '{}'".format(
        port_start
    )
    inbound_query = "select * from trips_final_1 where arrival = '{}'".format(
        port_start
    )
    outbound = read_pd_df(outbound_query)
    inbound = read_pd_df(inbound_query)
    col1 = outbound.time_start.dt.week if not outbound.empty else outbound.time_start
    col2 = inbound.time_start.dt.week if not inbound.empty else inbound.time_end
    return p.generate_stacked_histogram(col1, [], port_start, "Week", "Count")


@app.callback(
    Output("destination_port_traffic", "figure"), [Input("port_end", "value")]
)
def show_arrival_port_counts(port_end):
    outbound_query = "select * from trips_final_1 where departure = '{}'".format(
        port_end
    )
    inbound_query = "select * from trips_final_1 where arrival = '{}'".format(port_end)
    outbound = read_pd_df(outbound_query)
    inbound = read_pd_df(inbound_query)
    col1 = outbound.time_start.dt.week if not outbound.empty else outbound.time_start
    col2 = inbound.time_start.dt.week if not inbound.empty else inbound.time_end
    return p.generate_stacked_histogram([], col2, port_end, "Week", "Count")


@app.callback(
    Output("map", "figure"), [Input("port_start", "value"), Input("port_end", "value")]
)
def show_departure_port_on_map(port_start, port_end):
    return p.generate_map(port_list, port_start, port_end)


@app.callback(Output("heat_map_start", "figure"), [Input("port_start", "value")])
def show_port_heat_start(port_start):
    query = "select * from port_heat where \"PORT\" = '{}'".format(port_start)
    df = read_pd_df(query)
    return p.generate_heat_map(df, port_start, port_list)


@app.callback(Output("heat_map_end", "figure"), [Input("port_end", "value")])
def show_port_heat_end(port_end):
    query = "select * from port_heat where \"PORT\" = '{}'".format(port_end)
    df = read_pd_df(query)
    return p.generate_heat_map(df, port_end, port_list)


# start Flask server
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80, debug=False)
