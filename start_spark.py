import __main__

from os import environ
import json
from pyspark.sql import SparkSession

from spark_log import Log4j


def start_spark(
    app_name="DPL_SPARK_ETL", master="local[*]", jar_packages=[], files=[], spark_config={}
):
    '''
    Parameters
    ----------
    app_name : String
        Application name for the spark. The default is "DPL_SPARK_ETL".
    master : String
        Determine the run mode. The default is "local[*]".
    jar_packages : list of .jar files
        List of .jar files to be used to configure the spark
    files : list of .py files
        List of .py files to be used to configure the spark
    spark_config : dictionary
        dictionary of other configs to config the spark

    Returns
    -------
    spark_sess : Spark Session
    spark_logger : Spark Logger
    config_dict : dictionary
    '''
    # detect execution environment
    flag_repl = not (hasattr(__main__, "__file__"))
    flag_debug = "DEBUG" in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = SparkSession.builder.appName(app_name)
    else:
        # get Spark session factory
        spark_builder = SparkSession.builder.master(master).appName(app_name)

        # create Spark JAR packages string
        spark_jars_packages = ",".join(list(jar_packages))
        spark_builder.config("spark.jars.packages", spark_jars_packages)

        spark_files = ",".join(list(files))
        spark_builder.config("spark.files", spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    with open(files[0], "r") as config_file:
        config_dict = json.load(config_file)
    return spark_sess, spark_logger, config_dict
