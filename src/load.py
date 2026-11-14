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
    parquet_folder_path = config["parquet_folder_path"]

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
    parquet_folder_path = config["parquet_folder_path"]

    """(
        df.write.format("net.snowflake.spark.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", config["target_table"])
        .mode(config.get("write_mode", write_mode))
        .save()
    )"""

    print(f"Successfully loaded data into Snowflake table: {config['target_table']}")


def execute_load(config_json):

    target = os.getenv("LOAD_TARGET", "mysql").lower()

    try:

        # Read the JSON configuration file
        with open(config_json) as file:
            config = json.load(file)

        # Create a SparkSession
        spark = SparkInitializer.get_spark()

        if target == "mysql":

            db_config = config["mysql_config"]
            df = spark.read.parquet(db_config["parquet_folder_path"])
            load_data_mysql(df, db_config)

        elif target == "snowflake":

            sf_config = config["snowflake_config"]
            df = spark.read.parquet(sf_config["parquet_folder_path"])
            load_to_snowflake(df, sf_config)

    except FileNotFoundError as fe:
        raise Exception("Load file not found") from fe
    except json.JSONDecodeError as je:
        raise Exception("Invalid JSON load configuration") from je
    except Py4JJavaError as pje:
        raise Exception("Py4J Java error occurred") from pje


# Path to the JSON load configuration file
load_json = "Json_Folder/config.json"
execute_load(load_json)
