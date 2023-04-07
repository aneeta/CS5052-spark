from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from pyspark.sql.functions import sum, col

from webapp.components.maindash import app

from spark import data, la_data, LA_NAMES, SCHOOL_TYPES, PERIODS

from webapp.data import LA_GEOJSON

def new_la_map_component():
    title = html.H3('Enrolment over time')
    description = html.P("""
    Interactive and intuitive animated visualistation using external libraries
    as well as additional geographical data.
    """)
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
