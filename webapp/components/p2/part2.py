import dash_bootstrap_components as dbc
from dash import html

from webapp.components.p2.compare import compare_component
from webapp.components.p2.explore import explore_component

def get_part_two():
    return html.Div([
        dbc.Card(dbc.CardBody([compare_component()]), style={"margin-bottom": 10}),
        dbc.Card(dbc.CardBody([explore_component()]), style={"margin-bottom": 10})
    ], style={"padding": 20})

