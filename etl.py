"""
    Build the data pipelines to create the data models
    Author: khaidh1
    Date: 2022-07-25

Returns:
    parquet: the spark data models
"""

import pandas as pd
import configparser
import csv

# For Spark lib
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType as Double, StringType as Str, IntegerType as Int,\
    TimestampType as Timestamp, DateType as Date, LongType as Long
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import *

DATA_FOLDER = "./data/"
RESULT_FOLDER = "./data/result/"


config = configparser.ConfigParser()
SAS_LableFile_path = DATA_FOLDER + 'I94_SAS_Labels_Descriptions.SAS'
_List_I94_Factor = ["I94CIT & I94RES",
                    "I94PORT", "I94MODE", "I94ADDR", "I94BIR"]
_List_CSV_Extract_FileName = ['i94_country.csv', 'i94_port.csv',
                              'i94_model.csv', 'i94_state_addrl.csv', 'i94_visa.csv']
_List_CSV_Header = [['code', 'country_name'], ['code', 'port', 'state_code'], [
    'code', 'model'], ['code', 'state'], ['code', 'VISA_Type']]

nonUSstates = ['CANADA', 'Canada', 'NETHERLANDS', 'NETH ANTILLES', 'THAILAND', 'ETHIOPIA', 'PRC', 'BERMUDA', 'COLOMBIA', 'ARGENTINA', 'MEXICO',
               'BRAZIL', 'URUGUAY', 'IRELAND', 'GABON', 'BAHAMAS', 'MX', 'CAYMAN ISLAND', 'SEOUL KOREA', 'JAPAN', 'ROMANIA', 'INDONESIA',
               'SOUTH AFRICA', 'ENGLAND', 'KENYA', 'TURK & CAIMAN', 'PANAMA', 'NEW GUINEA', 'ECUADOR', 'ITALY', 'EL SALVADOR']


def create_Spark():
    """
        Create spark session
    """
    spark = SparkSession.builder.\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def extract_data_from_SAS_labels(input_label):
    '''
    A procedure that returns a cleaned list of code data pairs for the provided input label
    Parameters:
        Input:
        input_label : str
            name of the label in the SAS labels descriptions file

        Returns:
        code_data_list : list(tuple(str, str))
            a list of code data pairs extracted from the SAS labels descriptions file and cleaned
    '''
    with open(SAS_LableFile_path) as labels_descriptions:
        raw_labels = labels_descriptions.read()

    print("Process for", input_label)
    # extract only label data
    labels = raw_labels[raw_labels.index(input_label):]
    labels = labels[:labels.index(';')]
    # in each line remove unnecessary spaces and extract the code and its corresponding value
    lines = labels.splitlines()
    code_data_list = []
    # In case the input_label is I94PORT. This is speical dataset need 3 columm.
    if input_label == "I94PORT":
        for line in lines:
            try:
                code, data = line.split('=')
                code = code.strip().strip("'").strip('"')
                data = data.strip().strip("'").strip('"').strip()
                _value1, _value2 = data.split(',')
                _value1 = _value1.strip()
                _value2 = _value2.strip()
                code_data_list.append((code, _value1, _value2))
            except:
                pass
    else:
        for line in lines:
            try:
                code, data = line.split('=')
                code = code.strip().strip("'").strip('"')
                data = data.strip().strip("'").strip('"').strip()
                code_data_list.append((code, data))
            except:
                pass

    return code_data_list


def get_SAS_dataset():
    """
    Get the dataset from a given label then export CSV files.

    Parameters:
    Returns:
        Dataset: The CSV dataset.
    """
    print("Begin get SAS  end extraxt dataset")
    for index in range(len(_List_I94_Factor)):
        _name_file = DATA_FOLDER + _List_CSV_Extract_FileName[index]
        csvfile = open(_name_file, 'w', encoding='UTF8', newline='')
        writer = csv.writer(csvfile)
        writer.writerow(_List_CSV_Header[index])
        writer.writerows(extract_data_from_SAS_labels(_List_I94_Factor[index]))
        csvfile.close()
    print("Finish get SAS end extraxt dataset")


def process_fact_immigration(spark, input_data, output_data):
    """
    Process the sas7bdat from datadase files and run process to clean up the 
    dataset before store all to the database
    Parameters:
        spark -- spark session
        input_data -- localtion of input data
        output_data --  localtion of output data
    Returns: Parguet data frames

    """
    print("Begin process create fact immigration dataset")
    # Load the immigration data from base
    print("Loading the immigration data from the base dataset")

    df_immigration_sp = spark.read.parquet(input_data)

    i94port = process_i94portCodes(spark, DATA_FOLDER + 'i94_port.csv')
    i94port.createOrReplaceTempView("i94portCodes")
    country_code = process_countryCodes(spark, DATA_FOLDER + 'i94_country.csv')
    country_code.createOrReplaceTempView("countryCodes")
    df_immigration_sp.createOrReplaceTempView("immigration_table")
    print("Converting for arrival and departure data column")
    # Convert for arrival data column
    df_imigration_arr = df_immigration_sp.withColumn(
        "arrdate", df_immigration_sp["arrdate"].cast('int'))
    # Convert for departure data column
    df_imigration_dep = df_imigration_arr.withColumn(
        "depdate", df_imigration_arr["depdate"].cast('int'))

    df_imigration_dep.printSchema()
    df_imigration_dep.head(3)
    print("Converting for i94visa and i94mode data column")
    df_imigration_dep.createOrReplaceTempView("immigration_table")
    df_immigration = spark.sql("""SELECT *, CASE 
                                        WHEN i94visa = 1.0 THEN 'Business' 
                                        WHEN i94visa = 2.0 THEN 'Pleasure'
                                        WHEN i94visa = 3.0 THEN 'Student'
                                        ELSE 'N/A' END AS visa_type
                                    FROM immigration_table""")
    df_immigration.createOrReplaceTempView("immigration_table")
    df_immigration = spark.sql("""SELECT *, CASE 
                                        WHEN i94mode = 1.0 THEN 'Air' 
                                        WHEN i94mode = 2.0 THEN 'Sea'
                                        WHEN i94mode = 3.0 THEN 'Land'
                                        WHEN i94mode = 9.0 THEN 'Not reported'
                                        ELSE 'N/A' END 
                                        AS arrival_modes
                                    FROM immigration_table""")
    df_immigration.createOrReplaceTempView("immigration_table")
    df_immigration.show(2)

    # Remove all entries into the united states that weren't via air travel
    print("Remove all entries into the united states that weren't via air travel")
    spark.sql("""
                SELECT *
                FROM immigration_table
                WHERE i94mode = 1;
            """).createOrReplaceTempView("immigration_table")

    # drop rows where the gender values entered is undefined
    print("Drop rows where the gender values entered is undefined")
    spark.sql("""
                SELECT * FROM immigration_table 
                WHERE gender IN ('F', 'M');
            """).createOrReplaceTempView("immigration_table")

    # convert the arrival dates
    print("Converting the arrival dates")
    spark.sql("""
                SELECT *, date_add(to_date('1960-01-01'), arrdate) 
                AS arrival_date 
                FROM immigration_table;
            """).createOrReplaceTempView("immigration_table")
    # convert the departure dates
    print("Converting the departure dates")
    spark.sql("""SELECT *, CASE 
                        WHEN depdate >= 1.0 THEN date_add(to_date('1960-01-01'), depdate)
                        WHEN depdate IS NULL THEN NULL
                        ELSE 'N/A' END 
                        AS departure_date            
                FROM immigration_table""").createOrReplaceTempView("immigration_table")

    print("Filter up the arrival  modes")
    df_immigration = spark.sql("""SELECT *
                                FROM immigration_table
                                WHERE arrival_modes = 'Air' """)
    df_immigration.createOrReplaceTempView("immigration_table")
    df_immigration.show(2)

    # Use an inner join to drop invalid codes country of citizenship
    print("Use an inner join to drop invalid codes country of citizenship")
    spark.sql("""
                SELECT im.*, cc.country_name AS citizenship_country
                FROM immigration_table im
                INNER JOIN countryCodes cc
                ON im.i94cit = cc.code;
            """).createOrReplaceTempView("immigration_table")

    # country of residence
    print("for countr of residence")
    spark.sql("""
                SELECT im.*, cc.country_name AS residence_country
                FROM immigration_table im
                INNER JOIN countryCodes cc
                ON im.i94res = cc.code;
            """).createOrReplaceTempView("immigration_table")

    # Add entry_port names and entry port states
    print("Adding entry_port names and entry port states")
    spark.sql("""
                SELECT im.*, pc.port AS entry_port, pc.state_code AS entry_port_state
                FROM immigration_table im 
                INNER JOIN i94portCodes pc
                ON im.i94port = pc.code;
            """).createOrReplaceTempView("immigration_table")

    # Compute the age of each individual
    print("Computing the age of each individual")
    spark.sql("""
                SELECT *, (2016-biryear) AS age 
                FROM immigration_table;
            """).createOrReplaceTempView("immigration_table")
    # Insert the immigration fact data into a spark dataframe
    print("Inserting the immigration fact data into a spark dataframe")
    spark.sql(" SELECT  * FROM immigration_table").show(3)
    fact_immigration = spark.sql("""
                            SELECT 
                                cicid, 
                                citizenship_country,
                                residence_country,
                                arrival_date,
                                departure_date,
                                age,
                                gender,
                                TRIM(UPPER (entry_port)) AS city,
                                TRIM(UPPER (entry_port_state)) AS state,
                                visa_type,
                                visatype AS detailed_visa_type
                            FROM immigration_table;
                    """)
    spark.sql("""SELECT * FROM immigration_table""").show(2)
    # Saving the data in parquet format
    print("Saving the data in parquet format in a spark dataframe...")
    try:
        fact_immigration.write.parquet(output_data)
    except:
        print("Failed to write the data in parquet format")
        print("Please check output data folder")
        print("The fact_immigration can be already exists")
    print("Done to process create fact immigration dataset")


def process_dim_temperature(spark, input_data, output_data):
    """
    Process the dim temperature data

    Parameters:
        spark: A Spark session object
        input_data: Data to process
        output_data: Data to store
    """
    print("Processing create dim temperature dataset ..")
    # Load data base.Use pandas to load DataFame
    print("Loading data base,use pandas to load DataFame .....")
    df_temperature = pd.read_csv(input_data)
    # Keep only data for the United States
    df_temperature = df_temperature[df_temperature['Country']
                                    == 'United States'].copy()
    # Convert the date to datetime objects
    print("Converting the date to datetime objects")
    df_temperature['date'] = pd.to_datetime(df_temperature.dt)
    # Remove all dates prior to 1950
    print("Removing all dates prior to 1950")
    df_temperature = df_temperature[df_temperature['date']
                                    > "1950-01-01"].copy()
    # convert the city names to upper case
    print("Convert the city names to upper case")
    df_temperature.City = df_temperature.City.str.strip().str.upper()
    # convert the dataframes from pandas to spark
    spark_df_temperature = spark.createDataFrame(df_temperature)
    spark_df_temperature .createOrReplaceTempView("dim_temperature")
    # Insert the temperature dim data into a spark dataframe
    print("Insert the temperature dim data into a spark dataframe")
    dim_temperature = spark.sql("""
                                SELECT
                                DISTINCT date, city,
                                AVG(AverageTemperature) 
                                OVER (PARTITION BY date, City) AS average_temperature, 
                                AVG(AverageTemperatureUncertainty)
                                OVER (PARTITION BY date, City) AS average_termperature_uncertainty
                                FROM dim_temperature;
                            """)
    spark.sql("""SELECT * FROM dim_temperature""").show(2)
    # Saving the data in parquet format
    print("Saving the data in parquet format in a spark dataframe...")
    try:
        dim_temperature.write.parquet(output_data)
    except:
        print("Failed to write the data in parquet format")
        print("Please check output data folder")
        print("The dim_temperature can be already exists")

    print("Done to process temperature data")


def process_i94portCodes(spark, input_data):
    """
    Process i94 ports Code

    """
    df_i94_port = pd.read_csv(input_data)  # 'i94_port.csv'
    # remove all entries with null values as they are either un reported or outside the US
    df_i94port = df_i94_port[~df_i94_port.state_code.isna()].copy()
    df_i94port = df_i94port[~df_i94port.state_code.isin(nonUSstates)].copy()
    spark_df_i94portCodes = spark.createDataFrame(df_i94port)
    spark_df_i94portCodes.createOrReplaceTempView("i94portCodes")
    spark.sql("SELECT * FROM i94portCodes").show()
    return spark_df_i94portCodes


def process_countryCodes(spark, input_data):
    """ Process Country Code
    Args:
        spark (pyspark): Spark Session
        input_data (str): name of CSV files 

    Returns:
        Spark Session: 
    """
    df_i94_country = pd.read_csv(input_data)  # i94_country.csv'
    # Convert the data dictionaries to views in our spark context to perform SQL operations
    spark_df_country = spark.createDataFrame(df_i94_country)
    spark_df_country.createOrReplaceTempView("countryCodes")
    spark.sql("SELECT * FROM countryCodes").show()
    return spark_df_country


def process_dim_airports(spark, input_data, output_data: str):
    """
    Process the dim airports data
    Parameters:
        spark: A Spark session object
        input_data: Data to process
        output_data: Data to store
    """
    excluded_Values = ['closed', 'heliport', 'seaplane_base', 'balloonport']
    print("Processing create dim airport data...")
    # Load the csv directly into a spark dataframe
    print("Loading the various csv files into pandas dataframes..........")
    df_ariport_Code = pd.read_csv(input_data)
    # equivalent to the following pandas code:
    df_ariport_Code = df_ariport_Code[df_ariport_Code.iso_country.fillna(
        '').str.upper().str.contains('US')].copy()
    print("Process clean up datasets...")
    df_airports = df_ariport_Code[~df_ariport_Code['type'].str.strip().isin(
        excluded_Values)].copy()

    # Verify that the municipality field is available for all airports
    df_airports = df_airports[~df_airports['municipality'].isna()].copy()
    # Convert the municipality column to upper case in order to be able to join it with our other datasets.
    df_airports.municipality = df_airports.municipality.str.upper()
    df_airports['len'] = df_airports["iso_region"].apply(len)
    df_airports = df_airports[df_airports['len'] == 5].copy()

    # let's extract the state code
    print("Extract the state code of dataset")
    df_airports['state'] = df_airports['iso_region'].str.strip(
    ).str.split("-", n=1, expand=True)[1]
    print("Convert the dataframes from pandas to spark")
    print(type(df_airports))

    scheme_airports = StructType([StructField("indent", StringType(), True),
                                  StructField("type", StringType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("elevation_ft",
                                              DoubleType(), True),
                                  StructField("continent", StringType(), True),
                                  StructField("iso_country",
                                              StringType(), True),
                                  StructField(
                                      "iso_region", StringType(), True),
                                  StructField("municipality",
                                              StringType(), True),
                                  StructField("gps_code", StringType(), True),
                                  StructField("iata_code", StringType(), True),
                                  StructField(
                                      "local_code", StringType(), True),
                                  StructField("coordinates",
                                              StringType(), True),
                                  StructField("len", IntegerType(), True),
                                  StructField("state", StringType(), True),

                                  ])
    spark_df_ariports = spark.createDataFrame(
        df_airports, schema=scheme_airports)

    spark_df_ariports.createOrReplaceTempView("dim_airports")
    spark.sql("""SELECT * FROM dim_airports""").show(2)
    spark_df_ariports.printSchema()
    # Saving the data in parquet format
    print("Saving the data in parquet format in a spark dataframe...")
    try:
        spark_df_ariports.write.parquet(output_data)
    except:
        print("Failed to write the data in parquet format")
        print("Please check output data folder")
        print("The spark_df_ariports can be already exists")
    print("End process create airport data...")


def process_dim_demographics(spark, input_data, output_data):
    """
    Process the dim demographics data
    Parameters:
        spark: A Spark session object
        input_data: Data to process
        output_data: Data to store
    """
    print("Processing create dim demographics data ...")
    # load the various csv files into pandas dataframes
    print("Loading the various csv files into pandas dataframes..........")
    df_demographics = pd.read_csv(input_data, sep=';')
    # convert the city to upper case and remove any leading and trailing spaces
    df_demographics.City = df_demographics.City.str.upper().str.strip()
    # remove any leading or trailing spaces and convert to upper case
    df_demographics.City = df_demographics.City.str.strip().str.upper()
    print("Clearn up the dataset")
    # primary key will be the combination of city name and race
    df_demographics[df_demographics[['City', 'Race']].duplicated()]

    df_demographics['State Code'] = df_demographics['State Code'].str.strip(
    ).str.upper()
    df_demographics.Race = df_demographics.Race.str.strip().str.upper()

    # convert the dataframes from pandas to spark
    print("Covert the dataframes from pandas to spark")
    schema_demographics = StructType([StructField("City", StringType(), True),
                                     StructField("State", StringType(), True),
                                     StructField(
                                         "Median Age", DoubleType(), True),
                                     StructField("Male Population",
                                                 DoubleType(), True),
                                     StructField("Female Population",
                                                 DoubleType(), True),
                                     StructField("Total Population",
                                                 IntegerType(), True),
                                     StructField("Number of Veterans",
                                                 StringType(), True),
                                     StructField("Foreign-born",
                                                 StringType(), True),
                                     StructField(
                                         "Average Household Size", DoubleType(), True),
                                     StructField(
                                         "State Code", StringType(), True),
                                     StructField("Race", StringType(), True),
                                     StructField("Count", StringType(), True)

                                      ])
    spark_df_demographics = spark.createDataFrame(
        df_demographics, schema=schema_demographics)
    spark_df_demographics.createOrReplaceTempView("dim_demographics")
    # insert data into the demographics dim table
    print("Inserting data into the demographics dim table spark")
    dim_demographics = spark.sql("""
                                SELECT  `City` AS city, 
                                        `State` AS state, 
                                        `Median Age` AS median_age, 
                                        `Male Population` AS male_population, 
                                        `Female Population` AS female_population, 
                                        `Total Population` AS total_population, 
                                        `Foreign-born` AS foreign_born, 
                                        `Average Household Size` AS average_household_size, 
                                        `State Code` AS state_code, 
                                        `Race` AS race, 
                                        `Count` AS count
                                FROM dim_demographics
    """)
    spark.sql("""SELECT * FROM dim_demographics""").show(2)
    dim_demographics.printSchema()
    # Saving the data in parquet format
    print("Saving the data in parquet format in a spark dataframe...")
    try:
        dim_demographics.write.parquet(output_data)
    except:
        print("Failed to write the data in parquet format")
        print("Please check output data folder")
        print("The dim_demographics can be already exists")
    print("End process_dim_demographics")


def print_all_table_stats(spark):

    dim_demographics = spark.read.parquet(RESULT_FOLDER + "dim_demographics")
    dim_temperture = spark.read.parquet(RESULT_FOLDER + "dim_temperture")
    dim_airports = spark.read.parquet(RESULT_FOLDER + "dim_airports")
    fact_immagration = spark.read.parquet(RESULT_FOLDER + "fact_immagration")
    print("----------------------------------------------------------------")
    print("dim_demographics:")
    dim_demographics.printSchema()
    dim_demographics.head(1)
    print("----------------------------------------------------------------")
    print("dim_temperture:")
    dim_temperture.printSchema()
    dim_temperture.head(1)
    print("----------------------------------------------------------------")
    print("dim_airports:")
    dim_airports.printSchema()
    dim_airports.head(1)
    print("----------------------------------------------------------------")
    print("fact_immagration:")
    fact_immagration.printSchema()
    fact_immagration.head(1)
    print("----------------------------------------------------------------")


def main_etl(spark):
    """
    Main function: Run all process to create the ETL data pipelines
    Parameters:
        spark --- spark session
    """
    print("The process create the dim_demographics, dim_temperture, dim_airports and fact_immagration table .... ")
    print("----------------------------------------------------------------")
    process_dim_demographics(
        spark, DATA_FOLDER+"us-cities-demographics.csv", RESULT_FOLDER + "dim_demographics")
    print("----------------------------------------------------------------")
    process_dim_temperature(
        spark, DATA_FOLDER+"GlobalLandTemperaturesByCity.csv", RESULT_FOLDER+"dim_temperture")
    print("----------------------------------------------------------------")
    process_dim_airports(
        spark, DATA_FOLDER+"airport-codes_csv.csv", RESULT_FOLDER + "dim_airports")
    print("----------------------------------------------------------------")
    process_fact_immigration(
        spark, DATA_FOLDER+"sas_data", RESULT_FOLDER+"fact_immagration")
    print("Finish the ETL process")
    print("----------------------------------------------------------------")
    print_all_table_stats(spark)


if __name__ == '__main__':
    """
    The main function for etl process
    """
    print("Start the ETL process ......")
    print("----------------------------------------------------------------")
    print("Create the Spark Session...")
    spark = create_Spark()
    print_all_table_stats(spark)
    print("Finish the ETL process")
