from p1_main import explore

from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import dash_daq as daq


from dash.dependencies import Input, Output


import plotly.express as px



from pyspark.sql.functions import sum, col, desc, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

from webapp.components.maindash import app


from p1_main import explore

def explore_component():
    # data = explore().toPandas()
    # absences = dcc.Graph(id="absences-graph",
    #                      figure=px.line(data, x="Year", y="Overall Absence Rate (%)",
    #                                     color="Region", symbol="School Type",
    #                                     title="Absence Trends"))
    # absentees = dcc.Graph(id="absentees-graph",
    #                       figure=px.line(data, x="Year", y="Persistant Absentees Rate (%)",
    #                                     color="Region", symbol="School Type",
    #                                     title="Persistent Absentee Trends"))

    title = html.H3('Explore Performance over Time')
    description = html.P("""
    """)

    tabs = dcc.Tabs([
        dcc.Tab(label='Schools', children=[
            html.Div([daq.ToggleSwitch(
                label=['All', 'By Type'],labelPosition="top", id='schools-switch')],
                style={"padding": 10}),
            dcc.Graph(id="schools-graph")]),
        dcc.Tab(label='Enrolment', children=[
            html.Div([
                daq.ToggleSwitch(
                    label=['Total', 'School Average'],labelPosition="top", id='enrol-switch'),
                daq.ToggleSwitch(
                    label=['All', 'By Type'],labelPosition="top", id='enrol-switch-2'),
                ],
                style={"padding": 10}),
            dcc.Graph(id="enrol-graph")]),
        dcc.Tab(label='Absences', children=[
            html.Div([daq.ToggleSwitch(
                label=['All', 'By Type'],labelPosition="top", id='absences-switch')],
                style={"padding": 10}),
            dcc.Graph(id="absences-graph")]),
        dcc.Tab(label='Absentees', children=[
            html.Div([daq.ToggleSwitch(
                label=['All', 'By Type'],labelPosition="top", id='absentees-switch')],
                style={"padding": 10}),
            dcc.Graph(id="absentees-graph")]),
        # dcc.Tab(label='Absences', children=[absences]),
        # dcc.Tab(label='Absentees', children=[absentees])

    ])
    
    return html.Div([title, description, tabs], style={"padding": 20})

@app.callback(
    Output("absentees-graph", "figure"),
    Input('absentees-switch', "value"),
)
def _schools_figure(all):
    data = explore().where(col("School Type") == "Total") if not all else explore().where(col("School Type") != "Total") 
    data = data.toPandas()

    fig = px.line(data, x="Year", y="Persistant Absentees Rate (%)",
                                        color="Region", symbol="School Type",
                                        title="Persistent Absentee Trends")

    return fig

@app.callback(
    Output("absences-graph", "figure"),
    Input('absences-switch', "value"),
)
def _schools_figure(all):
    data = explore().where(col("School Type") == "Total") if not all else explore().where(col("School Type") != "Total") 
    data = data.toPandas()

    fig = px.line(data, x="Year", y="Overall Absence Rate (%)",
                                        color="Region", symbol="School Type",
                                        title="Absence Trends")

    return fig

@app.callback(
    Output("schools-graph", "figure"),
    Input('schools-switch', "value"),
)
def _schools_figure(all):
    data = explore().where(col("School Type") == "Total") if not all else explore().where(col("School Type") != "Total") 
    data = data.toPandas()

    fig = px.line(data, x="Year", y="Number of Schools",
                  color="Region", symbol="School Type",
                  title="Number of Schools Trend")

    return fig

@app.callback(
    Output("enrol-graph", "figure"),
    Input('enrol-switch', "value"),
    Input('enrol-switch-2', "value"),
)
def _enrol_figure(avg, all):
    data = explore().where(col("School Type") == "Total") if not all else explore().where(col("School Type") != "Total") 
    data = data.toPandas()
    if not avg:
        fig = px.line(data, x="Year", y="Enrolments",
                  color="Region", symbol="School Type",
                  title="Enrolment Trend")
    else:
        fig = px.line(data, x="Year", y="Average Enrolment",
                  color="Region", symbol="School Type",
                  title="Enrolment Trend")

    return fig
