from dash import dcc, html
import dash_bootstrap_components as dbc

def placeholder_component():
    title = html.H3('Placeholder')
    graph = dcc.Graph(
        figure={
            'data': [{
                'x': [1, 2, 3],
                'y': [3, 1, 2],
                'type': 'bar'
            }]})
    return dbc.Card(dbc.CardBody([html.Div([title, graph])]))