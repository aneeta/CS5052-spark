from dash import dcc, html
import plotly.express as px

from p1_main import get_nested_pie


def analyse():
    title = html.H3('Extended Data Analysis')
    description = html.P("""
    Hierarchy of school types collated with subtypes (types of establishment).
    """)
    source = dcc.Link("[Source]", href="https://www.get-information-schools.service.gov.uk/")
    graph2 = dcc.Graph(id="an-graph-2", figure = px.sunburst(get_nested_pie(), path=['school_type', 'TypeOfEstablishment (name)'], values='Total Enrolments'))
    return html.Div([title, description, source, graph2], style={"padding": 20})
