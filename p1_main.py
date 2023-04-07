from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc, avg, row_number, col, concat, substring, lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window

from pyspark.sql.types import LongType

from pyspark.ml.stat import ChiSquareTest, Correlation
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor, GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

import seaborn as sns

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

import matplotlib.pyplot as plt

import warnings

warnings.filterwarnings("ignore", category=UserWarning)



spark = SparkSession.builder.master("local") \
.appName("P1") \
.getOrCreate()

# # Part 1
# ## Read the dataset using Apache Spark.
data = spark.read \
.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.load("data/pupil-absence-in-schools-in-england_2018-19/data/Absence_3term201819_nat_reg_la_sch.csv")

la_data = data.where(col("geographic_level") == "Local authority").cache()

# ## Store the dataset using the methods supported by Apache Spark.
# data.persist()

# constants
LA_NAMES = sorted([i for i in data.select('la_name').distinct().rdd.map(
    lambda x: x.la_name).collect() if i != None])
PERIODS = data.select('time_period').distinct().rdd.map(
    lambda x: x.time_period).collect()
SCHOOL_TYPES = data.select('school_type').distinct().rdd.map(
    lambda x: x.school_type).collect()

# ## Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
# Given a list of local authorities, display in a well-formatted fashion
# the number of pupil enrolments in each local authority by time period
# (year).

def get_enrolments(local_authorities, year):
    return data.where(col("la_name").isin(local_authorities))\
        .where(col("time_period") == year)\
        .groupBy(["la_name", "time_period"])\
        .agg(sum("enrolments").alias("Total enrolment"))\
        .orderBy([col("la_name"), col("time_period")])\
        .select(col("la_name").alias("Local authority"), col("time_period").alias("Year"), col("Total enrolment"))

# ## Allow the user to search the dataset by school type, showing the total number of pupils who were given authorised absences because of medical appointments or illness in the time period 2017-2018.

def get_school_type():
    return data.where(col("geographic_level") == "National")\
                  .where(col('time_period') == 201718)\
                  .groupBy(["school_type"])\
                  .agg(
                    sum("sess_auth_illness").alias("Illness"),
                    sum("sess_auth_appointments").alias("Appointments"),)\
                  .withColumnRenamed('school_type', 'Type')

# ## Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.

def get_unauth_absences(year, view):
    gl = ["Local authority", "la_name", "Authority"] if view else ["Regional", "region_name", "Region"]
    return data.where(col("geographic_level") == gl[0])\
               .where(col("time_period") == year)\
               .groupBy(gl[1])\
               .orderBy(gl[1])\
                .agg(sum("sess_unauthorised").alias("All unauthorised absences"))\
               .withColumnRenamed(gl[1], gl[2])

# ## List the top 3 reasons for authorised absences in each year.

def get_top_reasons():
    top = data.where(col("geographic_level") == "National").groupBy('time_period').agg(
        sum("sess_auth_appointments").alias("Appointments"),
        sum("sess_auth_excluded").alias("Excluded"),
        sum("sess_auth_ext_holiday").alias("Extended_holiday"),
        sum("sess_auth_holiday").alias("Holiday"),
        sum("sess_auth_illness").alias("Illness"),
        sum("sess_auth_religious").alias("Religious"),
        sum("sess_auth_study").alias("Study"),
        sum("sess_auth_traveller").alias("Travel"),
        sum("sess_auth_other").alias("Other"),
    ).na.fill(0).withColumn("Extended_holiday", col("Extended_holiday").cast(LongType()))
    top_ = top.selectExpr("time_period",
    "stack( {}, ".format(
        str(len(top.columns) - 1)) + \
        ", ".join(["'{i}', {i}".format(i=i) for i in top.columns[1:]]) + \
        ") as (Reason, Count)")
    window = Window.partitionBy("time_period").orderBy(col("Count").desc())
    # Add a row number for each row within each time_period based on the rank column
    df = top_.withColumn("Rank", row_number().over(window))

    # Filter out the top 3 highest values in each time_period
    return df.filter(col("rank") <= 3)\
             .select(col("time_period")\
             .alias("Year"), "Rank", "Reason", "Count")
    

# # Part 2
# ## Allow a user to compare two local authorities of their choosing in a given year.
# Justify how you will compare and present the data.

def compare(loacal_authorities, year):
    return data.where(col("geographic_level") == "Local authority")\
                .where(col("la_name").isin(loacal_authorities))\
                .where(col('time_period') == year)\
                .groupBy(["la_name", "school_type"])\
                .agg(
                    sum("num_schools").alias("Number of Schools"),
                    sum("enrolments").alias("Enrolments"),
                    sum("enrolments_pa_10_exact").alias("Number of Persistent Absentees"),
                    sum("sess_overall").alias("Overall Absence Sessions"), # Number of overall absence sessions
                    sum("sess_possible").alias("Possible Sessions"), # Number of sessions possible
                )\
                .orderBy(["la_name", "school_type"])\
                .withColumn("Average Enrolment", col("Enrolments")/col("Number of Schools"))\
                .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
                .withColumn("Persistant Absentees Rate (%)", 100 * col("Number of Persistent Absentees")/col("Enrolments"))\
                .withColumnRenamed("school_type", "School Type")\
                .withColumnRenamed("la_name", "Authority")


# ## Chart/explore the performance of regions in England from 2006-2018.
# Your charts and subsequent analysis in your report should answer the
# following questions:
# * Are there any regions that have improved in pupil attendance over the years?
# * Are there any regions that have worsened?
# * Which is the overall best/worst region for pupil attendance?

def explore():
    return data.where(col("geographic_level") == "Regional")\
    .groupBy(["time_period","region_name", "school_type"])\
    .agg(
                sum("num_schools").alias("Number of Schools"),
                sum("enrolments").alias("Enrolments"),
                sum("enrolments_pa_10_exact").alias("Number of Persistent Absentees"),
                sum("sess_overall").alias("Overall Absence Sessions"), # Number of overall absence sessions
                sum("sess_possible").alias("Possible Sessions"), # Number of sessions possible
            )\
    .orderBy(["region_name","time_period", "school_type"])\
    .withColumn("Average Enrolment", col("Enrolments")/col("Number of Schools"))\
    .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
    .withColumn("Persistant Absentees Rate (%)", 100 * col("Number of Persistent Absentees")/col("Enrolments"))\
    .withColumnRenamed("school_type", "School Type")\
    .withColumnRenamed("region_name", "Region")\
    .withColumn("Year", concat(substring(col("time_period"), 1, 4), lit("/"), substring(col("time_period"), 5, 2)))

# # Part 3

# ## Analyse whether there is a link between school type, pupil absences and the location of the school.
# For example, is it more likely that schools of type X will have more pupil absences in location Y?

# ### Region Level
def get_region_analysis():
    return data.where(col("geographic_level") == "Regional")\
                .where(col("school_type") != "Total")\
                .groupBy(["region_name", "school_type"])\
                .agg(
                sum("num_schools").alias("Number of Schools"),
                sum("enrolments").alias("Enrolments"),
                sum("sess_overall").alias("Overall Absence Sessions"), # Number of overall absence sessions
                sum("sess_possible").alias("Possible Sessions"), # Number of sessions possible
                # sum("sess_possible_pa_10_exact") # Number of sessions possible of persistent absentees
            ).orderBy(["region_name"])\
                .withColumn("Average Enrolment", col("Enrolments")/col("Number of Schools"))\
                .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
                .withColumnRenamed("school_type", "School Type")\
                .withColumnRenamed("region_name", "Region")

def get_region_plots():
    print("""
    [Region level]
    It is more likely that Special schools will have more pupil absences.
    Location does not appear to have a strong relationship with regards to absences.

    Plots ("Region_Absence.png" and "Region_Type_Absence.png") saved to working directory.
    """)
    region_df = get_region_analysis()
    sns.set(rc={'figure.figsize':(8, 5)})
    plot_1 = sns.swarmplot(y='Region', x='Overall Absence Rate (%)', hue="School Type", data=region_df.toPandas())
    plot_1.get_figure().savefig("Region_Absence.png", bbox_inches="tight")
    plt.clf()
    plot_2 = sns.swarmplot(y='School Type', x='Overall Absence Rate (%)', hue='Region', data=region_df.toPandas())
    plot_2.get_figure().savefig("Region_Type_Absence.png", bbox_inches="tight")


# categorical_columns = ["Region", "School Type"] 

# indexers = [
#     StringIndexer(inputCol=c, outputCol=f"{c} Index")
#     for c in categorical_columns
# ]

# encoders = [
#     OneHotEncoder(inputCol=f"{c} Index", outputCol=f"{c} Vec")
#     for c in categorical_columns
# ]

# pipeline = Pipeline(stages=indexers + encoders)
# region_df_transformed = pipeline.fit(region_df).transform(region_df)

# region_df_transformed.corr("Region Index", 'Overall Absence Rate (%)')

# region_df_transformed.corr("School Type Index", 'Overall Absence Rate (%)')

# ### Local Authority Level

def get_la_analysis():
    return data.where(col("geographic_level") == "Local authority")\
    .where(col("school_type") != "Total")\
    .groupBy(["la_name", "school_type"])\
    .agg(
    sum("num_schools").alias("Number of Schools"),
    sum("enrolments").alias("Enrolments"),
    sum("sess_overall").alias("Overall Absence Sessions"), # Number of overall absence sessions
    sum("sess_possible").alias("Possible Sessions"), # Number of sessions possible
    # sum("sess_possible_pa_10_exact") # Number of sessions possible of persistent absentees
).orderBy(["la_name"])\
    .withColumn("Average Enrolment", col("Enrolments")/col("Number of Schools"))\
    .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
    .withColumnRenamed("school_type", "School Type")\
    .withColumnRenamed("la_name", "Local Authority")

def get_la_plots():
    print("""
    [Local Authority level]
    It is more likely that Special schools will have more pupil absences.
    Location does not appear to have a strong relationship with regards to absences.

    Plots ("LA_Absence.png" and "LA_Type_Absence.png") saved to working directory.
    """)
    plt.clf()
    la_df = get_la_analysis()
    plot_1 = sns.swarmplot(y='Local Authority', x='Overall Absence Rate (%)', hue="School Type", data=la_df.toPandas())
    fig = plt.gcf()
    fig.set_size_inches(5,20)
    fig.savefig("LA_Absence.png", bbox_inches="tight")
    plt.clf()
    plot_2 = sns.swarmplot(y='School Type', x='Overall Absence Rate (%)', data=la_df.toPandas())
    fig = plt.gcf()
    fig.set_size_inches(8,5)
    plot_2.get_figure().savefig("LA_Type_Absence.png", bbox_inches="tight")

data_institution = spark.read \
.format("csv") \
.option("inferSchema", "true") \
.option("header", "true") \
.load("data/edubasealldata20230323.csv")\
.withColumnRenamed("URN", "urn")

data_enriched = data.join(data_institution, on=["urn"], how="left")\
                    .where(col("geographic_level") == "School")\
                    .where(col("Gender (code)").isNotNull())\
                    .where(col("TypeOfEstablishment (code)").isNotNull())\
                    .where(col("PhaseOfEducation (code)").isNotNull())

### Fixed mappings (cat to numeric)
SCHOOL_TYPE_MAP = {
    "Special": 0,
    "State-funded primary": 1,
    "State-funded secondary": 2,
}
PHASE_MAP = {i["PhaseOfEducation (name)"]:i["PhaseOfEducation (code)"] for i in data_enriched.select("PhaseOfEducation (name)", "PhaseOfEducation (code)").distinct().collect()}
GENDER_MAP = {i["Gender (name)"]:i["Gender (code)"] for i in data_enriched.select("Gender (name)", "Gender (code)").distinct().collect()}
EST_MAP = {i["TypeOfEstablishment (name)"]:i["TypeOfEstablishment (code)"] for i in data_enriched.select("TypeOfEstablishment (name)", "TypeOfEstablishment (code)").distinct().collect()}
LA_MAP = {i["LA (name)"]:i["LA (code)"] for i in data_enriched.select("LA (name)", "LA (code)").distinct().collect()}

def encode_school_type(tier):
    return SCHOOL_TYPE_MAP[tier]

encode_tier_udf = udf(encode_school_type, IntegerType())

# Apply the UDF to encode the tiered categories
data_enriched = data_enriched.withColumn("school_type (code)", encode_tier_udf("school_type"))

to_encode = ["school_type", 'LA', 'TypeOfEstablishment', 'PhaseOfEducation', 'Gender']

encoders = [
    OneHotEncoder(inputCol=f"{c} (code)", outputCol=f"{c} Vec")
    for c in to_encode
]
feature_columns = to_encode + ["time_period"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# THROWS RuntimeError: SparkContext should only be created and accessed on the driver.
# pipeline = Pipeline(stages=[*encoders, assembler])
# data_model = pipeline.fit(data_enriched)

def fit_to_data_model(data):
    data_model = None
    for encoder in encoders:
        data = encoder.fit(data)
        data_model = encoder.transform(data)
    data = assembler.fit(data_model)
    return assembler.transform(data)

# data_model = fit_to_data_model(data_enriched)

def get_nested_pie():
    return data_enriched.groupBy(["school_type", "TypeOfEstablishment (name)",])\
             .agg(
                sum("enrolments").alias("Total Enrolments")
             ).toPandas()
def run_ml():
    print("""
    Build models to predict absence rate.

    To make predictions, navigate to the web app.
    """)


    train_data, test_data = fit_to_data_model(data_enriched).select("features", "sess_overall_percent").randomSplit([0.8, 0.2], seed=0)


    # Define the evaluator
    evaluator = RegressionEvaluator(labelCol="sess_overall_percent", predictionCol="prediction", metricName="rmse")

    # ### Linear regression
    lr = LinearRegression(featuresCol="features", labelCol="sess_overall_percent")

    data_institution.printSchema()
    param_grid_lr = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.1, 0.01, 0.001]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()


    cross_validator = CrossValidator(
        estimator=lr,
        estimatorParamMaps=param_grid_lr,
        evaluator=evaluator,
        numFolds=5
    )

    cv_model_lr = cross_validator.fit(train_data)
    best_lr_model = cv_model_lr.bestModel

    # predictions_lr = cv_model_lr.transform(test_data)
    glm = GeneralizedLinearRegression(featuresCol="features", labelCol="sess_overall_percent", family="gaussian", link="identity")
    param_grid_glm = ParamGridBuilder() \
        .addGrid(glm.regParam, [0.1, 0.01, 0.001]) \
        .addGrid(glm.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()
    # Train the model using the training data
    cross_validator_glm = CrossValidator(estimator=glm,
                                    estimatorParamMaps=param_grid_glm,
                                    evaluator=evaluator,
                                    numFolds=5)
    cv_model_glm = cross_validator_glm.fit(train_data)
    best_glm_model = cv_model_glm.bestModel
    # predictions_glm = best_glm_model.transform(test_data)
    # rmse_glm = evaluator.evaluate(predictions_glm)
    # cv_model_lr.bestModel.save("ml_model/glm")

    # print(f"[GLM] Root Mean Squared Error (RMSE) for the best model: {rmse_glm}")
    return best_lr_model, best_glm_model


def part_one():
    print("===Part 1===")
    print("""

    Give a list of local authorities to show pupil enrolments by time period.

    Type 'done' to finish querying.
    """)
    cont = True
    while cont:
        local_authorities = input("Enter local authorities (comma separated): ").split(",")
        if local_authorities[0].lower() == "done":
            cont = False
            continue
        year = int(input("Enter year (i.e. 200809): ").strip())
        la_stats = get_enrolments(local_authorities, year)
        la_stats.show(la_stats.count(), truncate=False)
    
    print("""

    Dataset by school type, showing the total number of
    authorised absences because of medical appointments
    or illness in the time period 2017-2018.
    """)

    school_type = get_school_type()
    school_type.show(school_type.count(), truncate=False)

    print("""

    Give year (in the format 200910 for 2009/10).

    Type 'done' to finish querying.
    """)
    cont = True
    while cont:
        year = input("Enter year : ").strip()
        if year.lower() == "done":
            cont = False
            continue
        breakdown = input("Enter granularity ('r' or 'la'): ").lower()
        view = True if breakdown == 'la' else False
        stats = get_unauth_absences(int(year), view)
        stats.show(stats.count(), truncate=False)
    
    print("""

    The top 3 reasons for authorised absences in each year.
    """)
    get_top_reasons().show(truncate=False)

def part_two():
    print("===Part 2===")
    print("""

    Compare two authorities in a given year.
    """)
    cont = True
    while cont:
        local_authorities = input("Enter local authorities (comma separated): ").split(",")
        if local_authorities[0].lower() == "done":
            cont = False
            continue
        year = int(input("Enter year (i.e. 200809): ").strip())
        compare(local_authorities, year).show()
    print("""

    Explore the dataset.
    """)

    print("Are there any regions that have improved in pupil attendance?")
    print("=TODO=")

    print("Are there any regions that have improved in pupil attendance?")
    print("=TODO=")

    print("Which is the overall worst region for pupil attendance?")
    explore().where(col('School Type') == 'Total')\
        .groupBy("Region")\
        .agg(
                sum("Overall Absence Sessions").alias("Overall Absence Sessions"), # Number of overall absence sessions
                sum("Possible Sessions").alias("Possible Sessions"), # Number of sessions possible
        )\
        .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
        .select("Region", "Overall Absence Rate (%)")\
        .orderBy("Overall Absence Rate (%)")\
        .limit(5).show()

    print("Which is the overall best region for pupil attendance?")
    explore().where(col('School Type') == 'Total')\
        .groupBy("Region")\
        .agg(
                sum("Overall Absence Sessions").alias("Overall Absence Sessions"), # Number of overall absence sessions
                sum("Possible Sessions").alias("Possible Sessions"), # Number of sessions possible
        )\
        .withColumn("Overall Absence Rate (%)", 100 * col("Overall Absence Sessions")/col("Possible Sessions"))\
        .select("Region", "Overall Absence Rate (%)")\
        .orderBy(col("Overall Absence Rate (%)").desc())\
        .limit(5).show()

def part_three():
    print("===Part 3===")
    get_region_plots()
    get_la_plots()
    run_ml()


def main():
    print("""
    Welcome to the terminal interface for CS5052 Practical 1.
    """)

    part_one()
    part_two()
    part_three()


if __name__ == "__main__":
    main()
