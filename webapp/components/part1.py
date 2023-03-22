from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px

from plotly.subplots import make_subplots


from pyspark.sql.functions import sum, col, desc

from webapp.components.maindash import app

from spark import data, la_data

from webapp.data import LA_GEOJSON

LA_NAMES = sorted([i for i in data.select('la_name').distinct().rdd.map(
    lambda x: x.la_name).collect() if i != None])
PERIODS = data.select('time_period').distinct().rdd.map(
    lambda x: x.time_period).collect()


def get_part_one():
    return html.Div([
        new_la_map_component(),
        la_enrolments_component()
    ])


def new_la_map_component():
    # slider = dcc.Slider(200607, 201819,
    #                     id='map-slider',
    #                     step=None,
    #                     marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
    #                            for i in PERIODS},
    #                     value=200607)

    # dropdown = dcc.Dropdown(options=[{'label': 'Total enrolment', 'value': 'enrolments'}, {'label': 'Total enrolment', 'value': 'enrolments'} ], value='enrolments', id='map-dropdown')
    title = html.H3('Map over time')
    graph = dcc.Graph(id="map-graph", figure=_new_la_map_figure())
    return html.Div([title, graph])


@app.callback(
    Output("map-graph", "figure"),
    Input('map-slider', "value")
)
def _new_la_map_figure():
    filtered_data = la_data.alias("a").groupBy(["la_name", "time_period"])\
        .agg(sum("enrolments").alias("Total enrolment"))\
        .join(data.alias("b").select("la_name", "new_la_code", "time_period"), col("a.la_name") == col("b.la_name"), "left")\
        .select(col("b.time_period").alias("Year"), col("b.new_la_code"), col("a.la_name"), col("Total enrolment"))\
        .distinct()\
        .orderBy(col("b.time_period"))\
        .toPandas()

    # Need for the labels on date slider to be formatted
    filtered_data['Year'] = filtered_data['Year'].apply(
        lambda x: "{}/{}".format(str(x)[:4], str(x)[4:]))

    fig = px.choropleth(
        filtered_data,
        geojson=LA_GEOJSON,
        animation_frame="Year",
        hover_name="la_name",
        locations="new_la_code",
        featureidkey="properties.reference",
        color="Total enrolment",
        scope='europe',
        labels={"new_la_code": "Local Authority Code"},
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    #                   title_text="Year {}/{}".format(str(period)[:4], str(period)[4:]))
    fig.update_geos(fitbounds="locations", visible=True, showcountries=True)
    return fig


def top_reasons_component():
    dropdown = dcc.Dropdown(LA_NAMES, [], id='la-dropdown', multi=True)
    # fig = _top_reasons_figure()
    title = html.H3('Top 3 Unauthorised Absences')
    graph = dcc.Graph(id="top-reasons-graph")
    return html.Div([title, graph])


# def _top_reasons_figure(local_authorities):
#     filtered_data = data.where(col("la_name").isin(local_authorities))
#     fig = go.Figure()
#     fig.add_trace(
#         go.Bar()
#     )
#     fig.update_layout(
#         updatemenus=[dict(

#         )]

#     )
#     return fig


def la_enrolments_component():
    dropdown = dcc.Dropdown(LA_NAMES, ['Barnet'], id='la-dropdown', multi=True)
    slider = dcc.Slider(200607, 201819,
                        id='date-slider',
                        step=None,
                        marks={i: '{}/{}'.format(str(i)[:4], str(i)[4:])
                               for i in PERIODS},
                        value=200607)
    # fig = _top_reasons_figure()
    title = html.H3('Local authority by Year')
    graph = dcc.Graph(id="la-enrol-graph")
    return html.Div([title, dropdown, graph, slider])


@ app.callback(
    Output("la-enrol-graph", "figure"),
    Input('la-dropdown', "value"),
    Input('date-slider', "value")
)
def _la_enrolments_figure(local_authorities, period):
    filtered_data = la_data.where(col("la_name").isin(local_authorities))\
        .where(col("time_period") == period)\
        .groupBy(["la_name"])\
        .agg(sum("enrolments").alias("Total enrolment"))\
        .orderBy([col("la_name").alias("Local authority")])\
        .select(col("la_name").alias("Local authority"), col("Total enrolment"))\
        .toPandas()
    # fig = make_subplots(rows=1, cols=2)
    fig = go.Figure()

    # fig.add_trace(
    #     go.Table(
    #         header=dict(values=list(filtered_data.columns),
    #                     fill_color='gray'),
    #         cells=dict(values=[filtered_data["Local authority"],
    #                            filtered_data["Total enrolment"]])
    #     ),
    #     row=1, col=1)

    fig.add_trace(
        go.Bar(x=filtered_data["Local authority"],
               y=filtered_data["Total enrolment"]))
    # row=1, col=1)

    return fig

# def la_enrolments_component():
#     dropdown = dcc.Dropdown(LA_NAMES, [], id='la-dropdown', multi=True)
#     fig = _top_reasons_figure()
#     title = html.H3('Top 3 Unauthorised Absences')
#     graph = dcc.Graph(id="top-reasons-graph")
#     return html.Div([title, graph])

# def _la_enrolments_figure(local_authorities):
#     filtered_data = data.where(col("la_name").isin(local_authorities))
#     fig = go.Figure()
#     fig.add_trace(
#         go.Table()
#     )
#     fig.update_layout(
#         updatemenus=[dict(

#         )]

#     )
#     return fig


def placeholder_component():
    title = html.H3('Placeholder')
    graph = dcc.Graph(
        figure={
            'data': [{
                'x': [1, 2, 3],
                'y': [3, 1, 2],
                'type': 'bar'
            }]
        }
    )
    return html.Div([title, graph])

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
