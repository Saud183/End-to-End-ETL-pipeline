import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger("validations")


def get_curr_date(spark):
    try:
        opDF = spark.sql("""select current_date""")
        logger.info("Validate the spark object by printing current date " + str(opDF.collect()))
    except NameError as exp:
        logger.error("Name error in method "+ str(exp),exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method "+ str(exp),exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark object is ready")

def df_count(df, dfName):
    try:
        logger.info(f"The dataframe validation by count df_count() is started for dataframe {dfName}")
        df_count = df.count()
        logger.info(f"The dataframe count is {df_count}")
    except Exception as exp:
        logger.error("Error in the method df_count() "+ str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The dataframe validation by df_count() is completed")

def df_top10_rec(df, dfName):
    try:
        logger.info(f"The dataframe validation by the top 10 records is started for dataframe {dfName}")
        logger.info(f"The dataframe top 10 records are ")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method df_top10_records" + str(exp))
        raise
    else:
        logger.info("The Dataframe validation by top10 records is completed")


def df_print_schema(df, dfName):
    try:
        logger.info(f"the Dataframe schema validation for dataframe {dfName}")
        sch = df.schema.fields
        logger.info(f"The dataframe {dfName } schema is ")
        for i in sch:
            logger.info(f"\t{i}")

    except Exception as exp:
        logger.error("Error in the method df_show_schema "+ str(exp), exc_info=True)
        raise
    else:
        logger.info("The Dataframe schema validation is completed")

