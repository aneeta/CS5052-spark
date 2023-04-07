# from dash import dcc, html
# import dash_bootstrap_components as dbc
# import dash_daq as daq
# from dash.dependencies import Input, Output
# import plotly.express as px

# from dash.dependencies import Input, Output, State


# from webapp.components.maindash import app
# from p1_main import (
#     LA_MAP, EST_MAP, PHASE_MAP, SCHOOL_TYPE_MAP, GENDER_MAP, PERIODS,
#     transform_to_predict, run_ml
#     )

# lr, glm = run_ml()

# MODEL_MAP = {
#     "Linear Regression": lr,
#     "GLM": glm,
# }

# YEARS_MAP = {i: '{}/{}'.format(str(i)[:4], str(i)[4:])
#                                for i in PERIODS + \
#                                 [201920, 202021, 202122, 202223, 202324]}



# def ml():
#     # toggle = html.Div([daq.ToggleSwitch(
#     #             label=['Local Authority', 'Region'],labelPosition="top", id='pd-switch')],
#     #             style={"padding": 10})
#     title = html.H3('Predict Absences')
#     description = html.P("""
#     Analysing whether there is a link between school type, pupil absences and 
#     the location of the school.
#     """)
#     input_form = html.Div(
#         [ dbc.Row([
#         dbc.Col([
#             html.H4("Input form"),
#             dcc.Dropdown(id="model-in", options=[{"label":k, "value":v} for k,v in MODEL_MAP.items()], placeholder="Model"),
#             dcc.Dropdown(id="year-in", options=[{"label":k, "value":v} for k,v in YEARS_MAP.items()], placeholder="Entry Year"),
#             dcc.Dropdown(id="school_type-in", options=[{"label":k, "value":v} for k,v in SCHOOL_TYPE_MAP.items()], placeholder="School type"),
            
#         ]),
#         dbc.Col([
#             dcc.Dropdown(id="la-in", options=[{"label":k, "value":v} for k,v in LA_MAP.items()], placeholder="Authority"),
#             dcc.Dropdown(id="est-in", options=[{"label":k, "value":v} for k,v in EST_MAP.items()], placeholder="Type of Establishment"),
#             dcc.Dropdown(id="phase-in", options=[{"label":k, "value":v} for k,v in PHASE_MAP.items()], placeholder="Phase of Education"),
#             dcc.Dropdown(id="gender-in", options=[{"label":k, "value":v} for k,v in GENDER_MAP.items()], placeholder="Gender"),
#         ]),
#         dbc.Button("Predict", id="add-row-button", color="primary", className="mt-2")
#     ]),
#     dbc.Row([
#         dbc.Col([
#             html.H4("Prediction"),
#             dcc.Graph(id="table", config={'displayModeBar': False})
#         ])
#     ])]
#     )

#     graph = dcc.Graph(id="pd-graph")
#     return html.Div([title, description, graph], style={"padding": 20})

# @app.callback(
#     Output("table", "figure"),
#     [Input("add-row-button", "n_clicks")],
#     [
#         State("model-in", "value"),
#         State("year-in", "value"),
#         State("school_type-in", "value"),
#         State("la-in", "value"),
#         State("est-in", "value"),
#         State("phase-in", "value"),
#         State("gender-in", "value"),
#     ],
# )
# def update_table(n_clicks, model, year, school, la, est, phase, gender):
#     new_row_values = (year, school, la, est, phase, gender)
#     global df
#     if n_clicks is not None and n_clicks > 0:
#         df = transform_to_predict(model, new_row_values).toPandas()

#     return {
#         "data": [dict(type='table',
#                       header=dict(values=list(df.columns),
#                                   fill_color='paleturquoise',
#                                   align='left'),
#                       cells=dict(values=[df[col] for col in df.columns],
#                                  fill_color='lavender',
#                                  align='left'))
#                  ]
#     }

# if __name__ == "__main__":
#     app.run_server(debug=True)
