import os
import json
from src.Spark_Initializer import SparkInitializer
from pyspark.sql import DataFrame
from py4j.protocol import Py4JJavaError


def load_data_mysql(df: DataFrame, config) -> None:
    # Extract extracturation parameters
    mysql_host = config["mysql_host"]
    mysql_port = config["mysql_port"]
    driver = config["driver"]
    mysql_database = config["mysql_database"]
    mysql_user = config["mysql_user"]
    mysql_password = config["mysql_password"]
    database_table = config["target_table"]

    # JDBC URL for MySQL connection
    mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"

    # Extract load parameters
    jdbc_properties = {
        "url": mysql_url,
        "driver": driver,
        "user": mysql_user,
        "password": mysql_password,
        "dbtable": database_table,
    }

    # Write the data to the target table using JDBC
    (df.write.format("jdbc").options(**jdbc_properties).mode("append").save())

    print(f"Successfully loaded data into Mysql table: {config['target_table']}")


def load_to_postgres(df: DataFrame, config) -> None:
    # Extract extracturation parameters
    pg_host = config["pg_host"]
    pg_port = config["pg_port"]
    driver = config["driver"]
    pg_database = config["pg_database"]
    pg_user = config["pg_user"]
    pg_password = config["pg_password"]
    database_table = config["target_table"]

    # JDBC URL for PostGres connection
    mysql_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

    # Extract load parameters
    jdbc_properties = {
        "url": mysql_url,
        "driver": driver,
        "user": pg_user,
        "password": pg_password,
        "dbtable": database_table,
    }

    # Write the data to the target table using JDBC
    (df.write.format("jdbc").options(**jdbc_properties).mode("append").save())

    print(f"Successfully loaded data into Postgres table: {config['target_table']}")


def load_to_snowflake(df: DataFrame, config) -> None:

    sf_options = {
        "sfURL": config["sf_url"],
        "sfUser": config["sf_user"],
        "sfPassword": config["sf_password"],
        "sfDatabase": config["sf_database"],
        "sfSchema": config["sf_schema"],
        "sfWarehouse": config["sf_warehouse"],
    }

    write_mode = config["write_mode"]

    # write data to snowflake
    (
        df.write.format("net.snowflake.spark.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", config["target_table"])
        .mode(config.get("write_mode", write_mode))
        .save()
    )

    print(f"Successfully loaded data into Snowflake table: {config['target_table']}")


def load_to_s3(df: DataFrame, config) -> None:

    s3_path = config["s3_parquet_path"]
    write_mode = config["write_mode"]

    # write data to AWS s3
    (df.write.mode(write_mode).parquet(s3_path))

    print(f"Successfully loaded data into S3: {s3_path}")


def load_to_s3_iceberg(df: DataFrame, config) -> None:

    catalog_name = config["catalog_name"]
    database_name = config["database_name"]
    table_name = config["table_name"]
    write_mode = config["write_mode"]

    # Iceberg Table identifier
    table_identifier = f"{catalog_name}.{database_name}.{table_name}"

    # Write data to Iceberg table in AWS s3
    (df.write.format("iceberg").mode(write_mode).save(table_identifier))

    print(f"Successfully loaded data into Iceberg table: {config['table_name']}")
    
def execute_load(config_json):

    target = os.getenv("LOAD_TARGET", "mysql").lower()

    try:

        # Read the JSON configuration file
        with open(config_json) as file:
            config = json.load(file)
        
        # Create a SparkSession
        if target == "s3_iceberg":
            # Get Iceberg Config
            iceberg_s3_config = config["iceberg_s3_config"]
            iceberg_conf = SparkInitializer.get_iceberg_config(
                catalog_name=iceberg_s3_config["catalog_name"],
                s3_bucket=iceberg_s3_config["s3_bucket_name"],
            )

            # Initialize spark with Iceberg configruation
            spark = SparkInitializer.get_spark(
                app="Iceberg Data Load", iceberg_config=iceberg_conf
            )

        else:
            spark = SparkInitializer.get_spark()

        # Load execution flow based on input conditions
        if target == "mysql":

            db_config = config["mysql_config"]
            df = spark.read.parquet(db_config["parquet_folder_path"])
            load_data_mysql(df, db_config)

        elif target == "postgres":

            db_config = config["postgres_config"]
            df = spark.read.parquet(db_config["parquet_folder_path"])
            load_to_postgres(df, db_config)

        elif target == "snowflake":

            sf_config = config["snowflake_config"]
            df = spark.read.parquet(sf_config["parquet_folder_path"])
            load_to_snowflake(df, sf_config)

        elif target == "s3":

            s3_config = config["s3_config"]
            df = spark.read.parquet(s3_config["parquet_folder_path"])
            load_to_s3(df, s3_config)

        elif target == "s3_iceberg":

            iceberg_s3_config = config["iceberg_s3_config"]
            df = spark.read.parquet(iceberg_s3_config["parquet_folder_path"])
            load_to_s3_iceberg(df, iceberg_s3_config)

    except FileNotFoundError as fe:
        raise Exception("Load file not found") from fe
    except json.JSONDecodeError as je:
        raise Exception("Invalid JSON load configuration") from je
    except Py4JJavaError as pje:
        raise Exception("Py4J Java error occurred") from pje


# Path to the JSON load configuration file
config_json = "config/config.json"
execute_load(config_json)
