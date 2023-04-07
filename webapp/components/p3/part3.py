import dash_bootstrap_components as dbc
from dash import html

from webapp.components.p3.map import new_la_map_component
from webapp.components.p3.analyse import analyse
from webapp.components.p3.predict import predict
# from webapp.components.p3.ml import ml


def get_part_three():
    return html.Div([
        dbc.Card(dbc.CardBody([new_la_map_component()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([analyse()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([predict()]), style={"margin-bottom": 10}),
        # NOTE THROWS RuntimeError: SparkContext should only be created and accessed on the driver.
        # dbc.Card(dbc.CardBody([ml()]), style={"margin-bottom": 10})
        
    ], style={"padding": 20})

