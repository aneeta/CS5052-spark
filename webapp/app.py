from dash import Dash, dcc, html

from dash.dependencies import Input, Output

from webapp.components.part1 import get_part_one

from webapp.components.maindash import app


TABS = {
    "part-1": get_part_one(),
    "part-2": get_part_one(),
    "part-3": get_part_one()
}


app.layout = html.Div([
    html.H1("CS5052 Practical"),
    dcc.Tabs(id="parts", value="part-1",
             children=[dcc.Tab(label="Part 1", value="part-1"),
                       dcc.Tab(label="Part 2", value="part-2"),
                       dcc.Tab(label="Part 3", value="part-3")]),
    html.Div(id="tabs-content")
], style={"padding": 20})


@app.callback(
    Output("tabs-content", "children"),
    Input("parts", "value")
)
def render_page(tab):
    return TABS[tab]


if __name__ == '__main__':
    app.run_server(debug=True)
