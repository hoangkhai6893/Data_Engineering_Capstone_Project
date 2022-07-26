
# For Spark lib
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType as Double, StringType as Str, IntegerType as Int,\
    TimestampType as Timestamp, DateType as Date, LongType as Long
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import *
from sqlalchemy import values


spark_table = ["dim_demographics", "dim_temperture",
               "dim_airports", "fact_immagration"]

DATA_FOLDER = "./data/"
RESULT_FOLDER = "./data/result/"


def create_Spark():
    """
        Create spark session
    """
    spark = SparkSession.builder.\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def check_NULL_Value(spark, input_table,):
    """
    This function performs null value checks on specific columns of given tables received as parameters
    Parameters:
        spark -- spark session
        input_table -- the spark table that need to check
    """
    print("Processing check null value for table")
    input_table.createOrReplaceTempView("check_null_table")
    for column in input_table.columns:
        print(column)
        value_result = spark.sql(f"""
                        SELECT COUNT(*) as NULL_count 
                        FROM check_null_table
                        WHERE {column} IS NULL
            """).show()
        print("--------------------------------------------------------------")


def check_dimension_table(spark, input_table):
    print("Check the dimension table")
    input_table.createOrReplaceTempView("check_dimension_table")
    value_result = spark.sql(f"""
                        SELECT COUNT(*) 
                        AS dimension_count 
                        FROM check_dimension_table
            """).show()


def check_inner_join_fact_immigration_table(spark):
    """
    Check the inner join the distinct combinations of city and state in our fact table 
    Parameters:
        spark: Spark instance
    """
    df_immigration_test = spark.read.parquet(
        RESULT_FOLDER + "fact_immagration")
    df_demographics_test = spark.read.parquet(
        RESULT_FOLDER + "dim_demographics")
    df_ariports_test = spark.read.parquet(RESULT_FOLDER + "dim_airports")

    df_immigration_test.createOrReplaceTempView("fact_immigration")
    df_demographics_test.createOrReplaceTempView("dim_demographics")
    df_ariports_test.createOrReplaceTempView("dim_airports")
    print("----------------------------------------------------------------")
    spark.sql("""
                SELECT COUNT(DISTINCT city, state)
                FROM fact_immigration
    """).show(2)
    print("----------------------------------------------------------------")
    spark.sql("""
        SELECT COUNT(*)
        FROM
        (
        SELECT DISTINCT city, state
        FROM fact_immigration
        ) fi
        INNER JOIN 
        (
        SELECT DISTINCT municipality, state
        FROM dim_airports 
        ) da
        ON fi.city = da.municipality
        AND fi.state = da.state
        """).show(2)
    print("----------------------------------------------------------------")
    spark.sql("""
        SELECT COUNT(*)
        FROM
        (
        SELECT DISTINCT city, state
        FROM fact_immigration
        ) fi
        INNER JOIN 
        (
        SELECT DISTINCT city, state_code
        FROM dim_demographics 
        ) da
        ON fi.city = da.city
        AND fi.state = da.state_code
    """).show(2)

    print("----------------------------------------------------------------")


def main_check(spark):
    """
    Run all check quality DATA process 
    """
    print("Processing check for null values ")
    for table in spark_table:
        print("----------------------------------------------------------------")
        print("Process for table", table)
        df_immigration_test = spark.read.parquet(RESULT_FOLDER + table)
        check_NULL_Value(spark, df_immigration_test)
        print("----------------------------------------------------------------")
        check_dimension_table(spark, df_immigration_test)
        print("----------------------------------------------------------------")
    print("Done to check for null values ")
    print("----------------------------------------------------------------")
    print("Processing check inner joint of fact_immigration table ability")
    check_inner_join_fact_immigration_table(spark)
    print("----------------------------------------------------------------")
    print("Done process quality data checking")


if __name__ == '__main__':

    print("Create the Spark Session...")
    spark = create_Spark()
    main_check(spark)
