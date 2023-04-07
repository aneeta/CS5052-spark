from dash import html

from webapp.components.p2.compare import compare_component
from webapp.components.p2.explore import explore_component

def get_part_two():
    return html.Div([
        compare_component(),
        explore_component(),

    ], style={"padding": 20})

