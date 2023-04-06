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

top = data.where(col("geographic_level") == "National").groupBy('time_period').agg(
    sum("sess_auth_appointments").alias("Appointments"),
    sum("sess_auth_excluded").alias("Excluded"),
    sum("sess_auth_ext_holiday").alias("Extended_holiday"),
    sum("sess_auth_holiday").alias("Holiday"),
    sum("sess_auth_illness").alias("Illness"),
    sum("sess_auth_religious").alias("Religious"),
    sum("sess_auth_study").alias("Study"),
    sum("sess_auth_traveller").alias("Travel"),
    sum("sess_auth_other").alias("Other"),
).na.fill(0).withColumn("Extended_holiday", col("Extended_holiday").cast(LongType()))

top_ = top.selectExpr("time_period",
"stack( {}, ".format(
    str(len(top.columns) - 1)) + \
    ", ".join(["'{i}', {i}".format(i=i) for i in top.columns[1:]]) + \
    ") as (Reason, Count)")

window = Window.partitionBy("time_period").orderBy(col("Count").desc())

# Add a row number for each row within each time_period based on the rank column
df = top_.withColumn("Rank", row_number().over(window))
filtered_data = df.filter(col("Rank") <= 3).select(col("time_period").alias("Year"), "Rank", "Reason", "Count").toPandas()


def top_reasons_component():
    title = html.H3('Top 3 Authorised Absences')
    description = html.P("""
    Shows the top 3 reasons for authorised absences in each year.
    The data is aggregated on the National level.
    """)
    slider = dcc.Slider(200607, 201819,
                        id='top-date-slider',
                        step=None,
                        marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
                               for i in PERIODS},
                        value=200607)
    graph = dcc.Graph(id="top-reasons-graph")
    return html.Div([title, description, graph, slider], style={"padding": 20})

@app.callback(
    Output("top-reasons-graph", "figure"),
    Input('top-date-slider', "value")
)
def _top_reasons_figure(year):
    data = filtered_data[filtered_data.Year == year]
    
    fig = go.Figure()
    fig.add_trace(
        go.Bar(x=data['Count'], y=data['Reason'],
               textposition="inside", orientation="h")
    )
    return fig
