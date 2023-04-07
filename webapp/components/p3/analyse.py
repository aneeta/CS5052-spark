from dash import dcc, html
from dash.dependencies import Input, Output

import plotly.express as px

from webapp.components.maindash import app
from p1_main import get_la_analysis, get_region_analysis, get_nested_pie


def analyse():
    # toggle = html.Div([daq.ToggleSwitch(
    #             label=['Local Authority', 'Region'],labelPosition="top", id='an-switch')],
    #             style={"padding": 10})
    title = html.H3('Extended Data Analysis')
    description = html.P("""
    Hierarchy of school types collated with subtypes (types of establishment).
    """)
    source = dcc.Link("[Source]", href="https://www.get-information-schools.service.gov.uk/")
    # graph = dcc.Graph(id="an-graph")
    graph2 = dcc.Graph(id="an-graph-2", figure = px.sunburst(get_nested_pie(), path=['school_type', 'TypeOfEstablishment (name)'], values='Total Enrolments'))
    return html.Div([title, description, source, graph2], style={"padding": 20})

@app.callback(
    Output("an-graph", "figure"),
    Input('an-switch', "value"),
)
def _an_absences_figure(switch):
    y = 'Local Authority' if switch else 'Region'
    data = get_la_analysis() if switch else get_region_analysis()
    data = data.toPandas()
    fig = px.scatter(data, y=y, x="Overall Absence Rate (%)", color="School Type")
    return fig
