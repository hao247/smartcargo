import plotly.graph_objs as go
import plotly.express as px

##########################################
# Plot functions for webpage compoenents #
##########################################


def generate_port_dict(port_list):
    loc = []
    port_dict = {}
    for index, port in port_list.iterrows():
        loc = [port["Y"], port["X"]]
        port_dict[port["PORT_NAME"]] = loc
    return port_dict


def generate_histogram(col, title, x_title, y_title):
    trace = go.Histogram(
        x = col,
        xbins=dict(
            #start=0,
            #end=30,
            size=3,
        ),
        autobinx=False,
    )
    layout = go.Layout(
        xaxis = dict(title=x_title),
        yaxis = dict(title=y_title),
        font=dict(size=20),
        height = 450,
        margin = {'r':10, 't':10, 'l':10, 'b':100},
    )
    fig = go.Figure(data=[trace], layout=layout)
    fig.update_xaxes(tickfont=dict(size=20))
    fig.update_yaxes(tickfont=dict(size=20))
    return fig


def generate_stacked_histogram(col1, col2, title, x_title, y_title):
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=col1, marker_color='orange', name='outbound'))
    fig.add_trace(go.Histogram(x=col2, marker_color='red', name='inbound'))
    fig.update_layout(
        barmode='stack',
        title_text=title,
        xaxis_title_text=x_title,
        yaxis_title_text=y_title,
        font=dict(size=20),
        margin={'r':10, 't':50, 'l':10, 'b':50},
        height=350,
    )
    fig.update_xaxes(tickfont=dict(size=20))
    fig.update_yaxes(tickfont=dict(size=20))
    return fig


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
        font=dict(size=20),
    )
    fig = go.Figure(
        data=[trace],
        layout=layout,
    )
    fig.update_layout(margin={'r':30, 't':30, 'l':30, 'b':30})
    return fig


def generate_map(ports, port_start, port_end):
    port_dict = generate_port_dict(ports)
    fig = px.scatter_mapbox(
            ports, 
            lat='Y',
            lon='X',
            hover_name='PORT_NAME',
            hover_data=['IMPORTS', 'EXPORTS'],
            color_discrete_sequence=['green'],
            zoom=2.5,
            height=400)

    if port_start != 'null' and port_start != 'Departure port':
        fig.add_trace(
                go.Scattermapbox(
                    lat=[port_dict[port_start][0]],
                    lon=[port_dict[port_start][1]],
                    mode='markers',
                    marker=go.scattermapbox.Marker(size = 12, color = 'orange'),
                    text=['Start'],
                    hoverinfo='none'
                    ))
    
    if port_end != 'null' and port_end != 'Destination port':
        fig.add_trace(
                go.Scattermapbox(
                    lat=[port_dict[port_end][0]],
                    lon=[port_dict[port_end][1]],
                    mode='markers',
                    marker=go.scattermapbox.Marker(size = 12, color = 'red'),
                    text=['End'],
                    hoverinfo='none'
                    ))
    
    fig.update_layout(
            showlegend=False,
            mapbox_style='white-bg',
            mapbox_layers=[
                {
                    'below':'traces',
                    'sourcetype':'raster',
                    'source':[
                        'https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}'
                    ]
                }
            ])
    fig.update_layout(margin={'r':5, 't':0, 'l':5, 'b':5})
    return fig


def generate_heat_map(df, port, port_list):
    port_dict = generate_port_dict(port_list)
    fig = go.Figure()
    if port != 'Departure port' and port != 'Destination port':
        lat = port_dict[port][0]
        lon = port_dict[port][1]
        fig = go.Figure(go.Densitymapbox(lat=df.LAT, lon=df.LON, radius=2, showscale=False))
        fig.update_layout(
                mapbox = {
                    'style': 'stamen-terrain',
                    'center': {'lon': lon, 'lat': lat},
                    'zoom': 11},
                showlegend = False)
        fig.update_layout(margin={'r':5, 't':0, 'l':5, 'b':5})
    return fig
