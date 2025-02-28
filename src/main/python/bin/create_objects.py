from pyspark.sql import SparkSession
import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger("create_objects")


def get_spark_object(envn, appName):

    try:
        logger.info(f"get_spark_objects is started. The '{envn}' envn is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master='yarn'

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except NameError as exp:
        logger.error("Name Error in the method "+ str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Name Error in the method "+ str(exp), exc_info=True)
        raise

    else:
        logger.info("Spark object is created")

    return spark