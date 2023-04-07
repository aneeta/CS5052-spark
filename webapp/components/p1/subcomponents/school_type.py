from dash import Dash, dcc, html

from dash.dependencies import Input, Output
import plotly.express as px


from pyspark.sql.functions import sum, col, desc

from webapp.components.maindash import app

from p1_main import data, SCHOOL_TYPES

def absences_by_school_type_component():
    title = html.H3('Authorised absences in 2017/18')
    description = html.P("""
    Search the dataset by school type, showing the total number
    of pupils who were given authorised absences because of medical
    appointments or illness in the time period 2017-2018.
    """)
    # dropdowns = html.Div(children=[
    #     html.Div(children=[html.Label("Absence Reason"), dcc.Dropdown(options=[{'label': 'Medical Appointments', 'value': 'sess_auth_appointments'}, {
    #                           'label': 'Illness', 'value': 'sess_auth_illness'}], value='enrolments', id='reason-dropdown', multi=True)]),
    #     html.Div(children=[html.Label("School Type"), dcc.Dropdown(options=SCHOOL_TYPES, value="Special", id='school-type-dropdown')])
    # ])
    dropdown = html.Div(
        children=[
            html.Div([html.Label("School Type")], style=dict(padding=10)),
            html.Div([dcc.Dropdown(options=SCHOOL_TYPES, value="Special", id='school-type-dropdown')],
                     style=dict(width='33%', padding=10))],
        style=dict(display='flex'))
    graph = dcc.Graph(id="abs-school-type-graph")
    return html.Div([title, description, dropdown, graph], style={"padding": 20})


@app.callback(
    Output("abs-school-type-graph", "figure"),
    Input('school-type-dropdown', "value")
)
def _absences_by_school_type_figure(school_type):

    filtered_data = data.where(col("geographic_level") == "National")\
                        .where(col('school_type') == school_type)\
                        .where(col('time_period') == 201718)\
        .groupBy(["school_type"])\
        .agg(
            sum("sess_auth_illness").alias("Illness"),
            sum("sess_auth_appointments").alias("Appointments"),
            ).toPandas()
    melted = filtered_data.melt(value_vars=['Illness', 'Appointments'])
    fig = px.pie(melted, values="value", names="variable", labels=melted.value)
    fig.update_traces(hoverinfo='label', textinfo='value+percent')
    return fig