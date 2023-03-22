from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window

def main():
    # init spark session
    spark = SparkSession.builder.master("local") \
                                .appName("Word Count") \
                                .config("conf-key", "conf-value") \
                                .getOrCreate()
    # TODO:
    # define schema
    # schema = StructType([
    #     StructField()
    # ])

    # load in data
    data = spark.read \
                .format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load("pupil-absence-in-schools-in-england_2018-19/data/Absence_3term201819_nat_reg_la_sch.csv")
    
    # RDD??

    # Persist the table

    # PART 1
    get_enrolment(data, ['North West' 'Yorkshire and the Humber' 'East Midlands'])


def get_enrolment(data, local_authorities: list):
    w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    data.withColumn('dummy_column', f.max('date').over(w).cast('timestamp')).show(9)
    # ret = data.select(sum("enrolments")) \
    ret = data.where(col("la_name").isin(local_authorities)) \
              .groupBy(["la_name", "time_period"]) \
              .agg(sum("enrolments").alias("Total enrolments"))
    return ret


def get_absence_reasons(data, n=3):
    data.select("enrolments").groupby


if __name__ == "__main__":
    main()