# -*- coding: utf-8 -*-
"""
Created on Sun Sep 12 13:18:56 2021

@author: Tung
"""
import os
from pyspark.sql.types import StructType, StructField, DoubleType, \
                            StringType, IntegerType
import pyspark.sql.functions as F
import boto3
import json
from pyspark.sql import SparkSession
import configparser

def set_aws_credentials(filename):
    '''
    Read AWS credentials from specified file.

    Parameters
    ----------
    filename : string
        Path to cfg file containing the credentials.

    Returns
    -------
    None.

    '''
    config = configparser.ConfigParser()
    config.read(filename)
    
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''    
    Create a new Spark session with additional configs.
    
    Returns
    -------
    spark: The created SparkSession object.
    
    '''
    spark = SparkSession \
        .builder \
        .appName('capstone') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.0") \
        .getOrCreate()

    # Settings to allow the parsing of some timestamps
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    return spark

def create_zone_df(spark, input_path):
    '''
    Constructs the zone DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    input_path : string
        The S3 bucket containing the zone data.

    Returns
    -------
    zone_df : DataFrame
        The zone DataFrame.

    '''
    zone_data = os.path.join(input_path, "taxi+_zone_lookup.csv")
    
    zoneSchema = StructType([
        StructField("location_id", IntegerType(), nullable=False),
        StructField("boro_name", StringType(), nullable=False),
        StructField("nta_name", StringType(), nullable=False),
        StructField("service_zone", StringType(), nullable=False)
    ])
    
    zone_df = spark.read.csv(zone_data, header=True, schema=zoneSchema)
    # Splitting happens here, see that the 3rd and 4th rows are almost the same.
    zone_df = zone_df.withColumn('nta_name', F.explode(F.split('nta_name', '/')))
    return zone_df

def create_temp_df(spark, input_path):
    '''
    Constructs the temp DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    input_path : string
        The S3 bucket containing the temperatures data.

    Returns
    -------
    temp_df : DataFrame
        The temp DataFrame.

    '''
    temp_data = os.path.join(input_path, "Hyperlocal_Temperature_Monitoring.csv")
    tempSchema = StructType([
        StructField("sensor_id", StringType(), nullable=False),
        StructField("air_temp", DoubleType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("hour", IntegerType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("install_type", StringType(), nullable=False),
        StructField("boro_name", StringType(), nullable=False),
        StructField("nta_code", StringType(), nullable=False)
    ])
    
    temp_df = spark.read.csv(temp_data, header=True, schema=tempSchema)
    # Convert original dates (in string) to timestamp format
    temp_df = temp_df.withColumn("date", F.to_timestamp("date", "M/dd/yyyy"))
    # Additional columns added to make comparisons with other DataFrames easier
    temp_df = temp_df.withColumn("month", F.month("date"))
    temp_df = temp_df.withColumn("day", F.dayofmonth("date"))
    return temp_df

def create_trips_df(spark, input_path):
    '''
    Constructs the trips DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    input_path : string
        The S3 bucket containing the trips data.

    Returns
    -------
    trips_df : DataFrame
        The trips DataFrame.

    '''
    trips_data = os.path.join(input_path, "yellow_tripdata_2018-07.csv")
    tripsSchema = StructType([
        StructField("vendor_id", IntegerType(), nullable=True),
        StructField("PU_date", StringType(), nullable=True),
        StructField("DO_date", StringType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("ratecode_id", IntegerType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("PU_location_id", IntegerType(), nullable=True),
        StructField("DO_location_id", IntegerType(), nullable=True),
        StructField("payment_type", IntegerType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("tolls_amount", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True)
    ])
    
    trips_df = spark.read.csv(trips_data, header=True, schema=tripsSchema)
    # Convert strings to timestamps
    trips_df = trips_df.withColumn("PU_date", F.to_timestamp("PU_date", "M/d/yyyy H:mm"))
    trips_df = trips_df.withColumn("DO_date", F.to_timestamp("DO_date", "M/d/yyyy H:mm"))
    # Add additional columns
    trips_df = trips_df.withColumn("month", F.month("PU_date"))
    trips_df = trips_df.withColumn("PU_day", F.dayofmonth("PU_date"))
    trips_df = trips_df.withColumn("DO_day", F.dayofmonth("DO_date"))
    trips_df = trips_df.withColumn("PU_hour", F.hour("PU_date"))
    trips_df = trips_df.withColumn("DO_hour", F.hour("DO_date"))
    return trips_df

def create_nta_df(spark, input_path):
    '''
    Constructs the nta DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    input_path : string
        The S3 bucket containing the nta data.

    Returns
    -------
    nta_df : DataFrame
        The nta DataFrame.

    '''
    s3 = boto3.resource('s3')
    content_object = s3.Object('tung99-bucket', 'nta_codes.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    nta_data = json_content["data"]
    
    nta_list = []    
    for nta in nta_data:
        for sub_nta in nta[13].split('-'):
            nta_list.append((nta[11], nta[12], sub_nta))
        
    rdd = spark.sparkContext.parallelize(nta_list)
    ntaSchema = StructType([
        StructField("boro_name", StringType(), nullable=False),
        StructField("nta_code", StringType(), nullable=False),
        StructField("nta_name", StringType(), nullable=False)
    ])
    nta_df = spark.createDataFrame(rdd, schema=ntaSchema)
    return nta_df

# Write to parquet operations temporarily commented out to save running time
# (these can take hours to finish)
def create_time_dim_table(spark, temp_df, output_path):
    '''
    Create the time dimension table from the DataFrames generated above.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    temp_df : DataFrame
        The temp DataFrame.
    output_path : TYPE
        S3 bucket to write the parquets to.

    Returns
    -------
    time_table : DataFrame
        The resulting time dimension table.

    '''
    temp_df.createOrReplaceTempView("temperatures")
    time_table = spark.sql('''
        SELECT year(date) AS year, month(date) AS month, dayofmonth(date) AS day, dayofweek(date) AS weekday, hour
        FROM temperatures
        WHERE year(date)=2018 AND month(date)=7
    ''').distinct().withColumn('time_id', F.monotonically_increasing_id())
    # times_output = os.path.join(output_path, "times")
    # time_table.write.parquet(times_output, mode='overwrite', partitionBy='weekday')
    return time_table

def create_trips_fact_table(spark, trips_df, time_table, output_path):
    '''
    Create the trips fact table from the DataFrames generated above.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    trips_df : DataFrame
        The trips DataFrame.
    time_table : DataFrame
        The time dimension table created above.
    output_path : TYPE
        S3 bucket to write the parquets to.

    Returns
    -------
    trips_table : DataFrame
        The resulting trips fact table.

    '''
    time_table.createOrReplaceTempView("times")
    trips_df.createOrReplaceTempView("trips")
    trips_table = spark.sql('''
        SELECT vendor_id, times.time_id AS PU_date_id, DO_day, t.month, 
                DO_hour, passenger_count, trip_distance, PU_location_id, 
                DO_location_id, payment_type, fare_amount, extra, mta_tax,
                tip_amount, tolls_amount, improvement_surcharge, total_amount
        FROM trips t
        JOIN times ON t.PU_day=times.day AND t.month=times.month AND t.PU_hour=times.hour
    ''')
    
    trips_table.createOrReplaceTempView("trips")
    trips_table = spark.sql('''
        SELECT vendor_id, PU_date_id, times.time_id AS DO_date_id, passenger_count, 
                trip_distance, PU_location_id, DO_location_id, payment_type, 
                fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
                improvement_surcharge, total_amount
        FROM trips t
        JOIN times ON t.DO_day=times.day AND t.month=times.month AND t.DO_hour=times.hour
    ''')
    # trips_output = os.path.join(output_path, "trips")
    # trips_table.write.parquet(trips_output, mode='overwrite', partitionBy=['PU_location_id', 'DO_location_id'])
    return trips_table

def create_loc_dim_table(spark, nta_df, zone_df, output_path):
    '''
    Create the locations dimension table from the DataFrames generated above.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    nta_df : DataFrame
        The nta DataFrame.
    zone_df : DataFrame
        The zone DataFrame.
    output_path : TYPE
        S3 bucket to write the parquets to.

    Returns
    -------
    loc_table : DataFrame
        The resulting locations dimension table.

    '''
    zone_df.createOrReplaceTempView("zones")
    nta_df.createOrReplaceTempView("ntas")
    loc_table = spark.sql('''
        SELECT location_id, z.boro_name, nta_code, z.nta_name, service_zone
        FROM zones z
        JOIN ntas ON z.nta_name=ntas.nta_name
    ''')
    # locations_output = os.path.join(output_path, "locations")
    # loc_table.write.parquet(locations_output, mode='overwrite', partitionBy='service_zone')
    return loc_table

def create_temps_fact_table(spark, temp_df, loc_table, output_path):
    '''
    Create the temperatures fact table from the DataFrames generated above.

    Parameters
    ----------
    spark : SparkSession
        The active SparkSession object.
    temp_df : DataFrame
        The temp DataFrame.
    loc_table : DataFrame
        The locations dimension table created above.
    output_path : TYPE
        S3 bucket to write the parquets to.

    Returns
    -------
    temps_table : DataFrame
        The resulting temps fact table.

    '''
    loc_table.createOrReplaceTempView("locations")
    temp_df.createOrReplaceTempView("temperatures")
    temps_table = spark.sql('''
        SELECT times.time_id, l.location_id, t.air_temp, t.install_type
        FROM temperatures t
        JOIN times ON times.month=t.month AND times.day=t.day AND times.hour=t.hour
        JOIN locations l ON l.nta_code=t.nta_code
    ''')    
    # temperatures_output = os.path.join(output_path, "temperatures")
    # temps_table.write.parquet(temperatures_output, mode='overwrite', partitionBy='location_id')
    return temps_table

# Define quality check functions here
def table_rows_check(table, num_rows):
    '''
    Returns True if table has exactly num_rows rows, otherwise returns False.

    Parameters
    ----------
    table : DataFrame
        A pyspark table.
    num_rows : int
       Number of rows the table should have.

    '''
    return table.count() == num_rows

def null_values_check(table_list):
    '''
    Returns True if there is not a single Null or NaN values in all tables, 
    otherwise returns False

    Parameters
    ----------
    table_list : list
        List of tables to check.

    '''    
    for table in table_list:
        # Check for NULL and NaN in every column of each table
        null_count = table.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) \
                                   for c in table.columns]).rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
        if null_count != 0:
            return False
    return True