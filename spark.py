from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col, desc


# init spark session
spark = SparkSession.builder.master("local") \
                            .appName("Word Count") \
                            .config("conf-key", "conf-value") \
                            .getOrCreate()

# Part 1: Read the dataset using Apache Spark
data = spark.read \
            .format("csv") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .load("data/pupil-absence-in-schools-in-england_2018-19/data/Absence_3term201819_nat_reg_la_sch.csv")

# Part 2: Store the dataset using the methods supported by Apache Spark.
la_data = data.where(col("geographic_level") == "Local authority").cache()
