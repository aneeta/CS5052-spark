from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px

from plotly.subplots import make_subplots


from pyspark.sql.functions import sum, col, desc, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

from webapp.components.maindash import app

from spark import data, la_data, LA_NAMES, SCHOOL_TYPES, PERIODS

from p1_main import compare

def compare_component():
    title = html.H3('Compare Local Authorities')
    description = html.P("""
    """)
    dropdown = dcc.Dropdown(
        options=LA_NAMES,
        value=['Liverpool', 'Hackney'],
        id='compare-dropdown',
        multi=True)
    slider = dcc.Slider(200607, 201819,
                        id='compare-slider',
                        step=None,
                        marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
                               for i in PERIODS},
                        value=200607)
    graph = dcc.Graph(id="compare-graph")
    return html.Div([title, description, dropdown, slider, graph], style={"padding": 20})

@app.callback(
    Output("compare-graph", "figure"),
    Input('compare-dropdown', "value"),
    Input('compare-slider', "value")
)
def _compare_figure(loacal_authorities, year):
    color_map = {
        'Total': '#9b989e',
        'Special': '#610bb8',
        'State-funded primary': '#8e60bd',
        'State-funded secondary': '#7938ba'
    }
    data = compare(loacal_authorities, year).toPandas()
    #.where(col("School Type"))

    fig = make_subplots(rows=3, cols=1,
                        subplot_titles=("Number of Schools", "Enrolments", "Absence Rates")
                        )
    school = px.bar(data, x="Authority", y="Number of Schools",
                    color="School Type", legendgroup = '1'
                    )
    enrol = px.bar(data, x="Authority", y="Enrolments",
                    color="School Type", legendgroup = '1'
                    )
    absences = px.bar(data, x="Authority", y="Overall Absence Rate (%)",
                    color="School Type", legendgroup = '1'
                    )
    for t in school.data:
        fig.append_trace(t, row=1, col=1)
    for t in enrol.data:
        fig.append_trace(t, row=2, col=1)
    for t in absences.data:
        fig.append_trace(t, row=3, col=1)
    # fig.add_trace(
    #     list(.select_traces()),
    #     row=1, col=1
    # )
    # fig.add_trace(
    #     list(px.bar(data, x="Authority", y="Enrolments",
    #                 color="School Type"
    #                 ).select_traces()),
    #     row=1, col=2
    # )
    # fig.add_traces(
        # list(px.bar(data, x="Authority", y="Enrolments",
        #             color="School Type"
        #             ).select_traces()),
    #     list(px.bar(data, x="Authority", y="Average Enrolment",
    #                 color="School Type"
    #                 ).select_traces()),
    #     cols=2)
    # fig.append_trace(
    #     go.Bar(x=data["Authority"], y=data["Number of Schools"],
    #            marker_color=list(color_map.values())
    #             # title="Number of Schools"
    #             ),
    #     row=1, col=1)

    fig.update_layout(height=1000, width=800, title_text="Comparison Plots")

    return fig
