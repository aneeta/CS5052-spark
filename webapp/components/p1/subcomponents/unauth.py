from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import dash_daq as daq


from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px



from pyspark.sql.functions import sum, col, desc

from webapp.components.maindash import app

from p1_main import data, LA_NAMES, SCHOOL_TYPES, PERIODS


def unauth_absences_component():
    title = html.H3('Unauthorised absences by year')
    description = html.P("""
    Search for all unauthorised absences in a certain year,
    broken down by either region name or local authority name.
    """)
    switch = daq.ToggleSwitch(
        label=['Region', 'Local authority'],
        labelPosition="top",
        id='unauth-switch'
    )
    slider = dcc.Slider(200607, 201819,
                        id='unauth-date-slider',
                        step=None,
                        marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
                               for i in PERIODS},
                        value=200607)
    graph = dcc.Graph(id="unauth-graph")
    return html.Div([title, description, switch, graph, slider], style={"padding": 20})

@app.callback(
    Output("unauth-graph", "figure"),
    Input('unauth-date-slider', "value"),
    Input('unauth-switch', "value")
)
def _unauth_absences_figure(date, view):
    gl = ["Local authority", "la_name", "Authority"] if view else ["Regional", "region_name", "Region"]
    filtered_data = data.where(col("geographic_level") == gl[0]).where(col("time_period") == date).groupBy(gl[1]).agg(
     sum("sess_unauthorised").alias("All unauthorised absences"),
    ).withColumnRenamed(gl[1], gl[2]).toPandas()
    fig = px.bar(filtered_data, x="All unauthorised absences", y=gl[2])
    return fig