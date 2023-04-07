from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import dash_daq as daq


from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px

from plotly.subplots import make_subplots


from pyspark.sql.functions import sum, col, desc

from webapp.components.maindash import app

from spark import data, la_data, LA_NAMES, SCHOOL_TYPES, PERIODS

from p1_main import get_la_analysis, get_region_analysis

def analyse():
    toggle = html.Div([daq.ToggleSwitch(
                label=['Local Authority', 'Region'],labelPosition="top", id='an-switch')],
                style={"padding": 10})
    title = html.H3('Link between absences and school type or location')
    description = html.P("""
    Analysing whether there is a link between school type, pupil absences and 
    the location of the school.
    """)
    graph = dcc.Graph(id="an-graph")
    return html.Div([title, description, toggle, graph], style={"padding": 20})

@app.callback(
    Output("an-graph", "figure"),
    Input('an-switch', "value"),
)
def _unauth_absences_figure( switch):
    y = 'Local Authority' if switch else 'Region'
    data = get_la_analysis() if switch else get_region_analysis()
    data = data.toPandas()
    fig = px.scatter(data, y=y, x="Overall Absence Rate (%)", color="School Type")
    return fig