from dash import dcc, html

from dash.dependencies import Input, Output

from webapp.components.p1.part1 import get_part_one
from webapp.components.p2.part2 import get_part_two
from webapp.components.p3.part3 import get_part_three

from webapp.components.maindash import app


TABS = {
    "part-1": get_part_one(),
    "part-2": get_part_two(),
    "part-3": get_part_three()
}


app.layout = html.Div([
    html.H1("CS5052 Practical"),
    dcc.Tabs(id="parts", value="part-1",
             children=[dcc.Tab(label="Part 1", value="part-1"),
                       dcc.Tab(label="Part 2", value="part-2"),
                       dcc.Tab(label="Part 3", value="part-3")]),
    html.Div(id="tabs-content")
], style={"padding": 25})


@app.callback(
    Output("tabs-content", "children"),
    Input("parts", "value")
)
def render_page(tab):
    return TABS[tab]


if __name__ == '__main__':
    app.run_server(debug=True)
