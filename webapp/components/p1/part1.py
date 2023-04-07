from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px

from plotly.subplots import make_subplots


from pyspark.sql.functions import sum, col, desc

from webapp.components.maindash import app

from spark import data, la_data, LA_NAMES, SCHOOL_TYPES, PERIODS

from webapp.components.p1.subcomponents.enrolments import la_enrolments_component
from webapp.components.p1.subcomponents.top_reasons import top_reasons_component
from webapp.components.p1.subcomponents.school_type import absences_by_school_type_component
from webapp.components.p1.subcomponents.unauth import unauth_absences_component

def get_part_one():
    return html.Div([
        dbc.Card(dbc.CardBody([la_enrolments_component()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([absences_by_school_type_component()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([unauth_absences_component()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([top_reasons_component()]), style={"margin-bottom": 10}),
    ], style={"padding": 20})


    # x = data.where(col("la_name").isin([None]))\
    #     # .where(col("time_period") == 200910)\
    # .groupBy(["la_name"])\
    #     .agg(sum("enrolments").alias("Total enrolment"))\
    #     .orderBy(["la_name"])\
    #     .select(col("la_name").alias("Local authority"), col("Total enrolment"))\
    #     .toPandas()


filtered_data = data.alias("a")\
    .where(col("geographic_level") == "Local authority")\
    .where(col("time_period") == 201819)\
    .groupBy(["la_name"])\
    .agg(sum("enrolments").alias("Total enrolment"))\
    .orderBy([col("la_name").alias("Local authority")])\
    .join(data.alias("b").select("la_name", "new_la_code").distinct(), col("a.la_name") == col("b.la_name"), "left")\
    .select(col("b.new_la_code"), col("a.la_name"), col("Total enrolment"))\
    .toPandas()
