from dash import dcc, html
from dash.dependencies import Input, Output

import plotly.graph_objects as go

from pyspark.sql.functions import sum, col

from webapp.components.maindash import app

from p1_main import la_data, LA_NAMES, PERIODS

def la_enrolments_component():
    dropdown = dcc.Dropdown(
        options=LA_NAMES,
        value=['Barnet', 'Camden', 'Southwark'],
        id='la-dropdown',
        multi=True)
    slider = dcc.Slider(200607, 201819,
                        id='date-slider',
                        step=None,
                        marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
                               for i in PERIODS},
                        value=200607)
    title = html.H3('Enrolments by Local authority and Year')
    description = html.P("""
    Search the dataset by the local authority.
    Shows the number of pupil enrolments in each local authority by time period (year).
    """)
    graph = dcc.Graph(id="la-enrol-graph")
    return html.Div([title, description, dropdown, graph, slider], style={"padding": 20})


@app.callback(
    Output("la-enrol-graph", "figure"),
    Input('la-dropdown', "value"),
    Input('date-slider', "value"),
)
def _la_enrolments_figure(local_authorities, period):
    filtered_data = la_data.where(col("la_name").isin(local_authorities))\
        .where(col("time_period") == period)\
        .groupBy(["la_name"])\
        .agg(sum("enrolments").alias("Total enrolment"))\
        .orderBy([col("la_name").alias("Local authority")])\
        .select(col("la_name").alias("Local authority"), col("Total enrolment"))\
        .toPandas()
    fig = go.Figure()

    fig.add_trace(
            go.Bar(x=filtered_data["Local authority"],
                y=filtered_data["Total enrolment"]))
    return fig
