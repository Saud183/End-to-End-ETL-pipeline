#importing all necessary modules
from crypt import methods

import get_all_variables as gav
from create_objects import get_spark_object
from src.main.python.bin.get_all_variables import inferScehma
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers

import sys
import logging
import logging.config
import os

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')

inferScehma = gav.inferScehma
header = gav.header

def main():
    try:
        logging.info("main() is started")
        #Get Spark object
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info("Object created")
        #Validate Spark Object
        get_curr_date(spark)

        # Initiate presc_run_data_ingest Script
        # Load the City File
        # for file in os.listdir(gav.staging_dim_city):
        #     print("File is " + file)
        #     file_dir = gav.staging_dim_city + '/' + file
        #     print(file_dir)
        #
        #     if file.split('.')[1] == 'csv':
        #         file_format = 'csv'
        #         header = gav.header
        #         inferSchema = gav.inferScehma
        #     elif file.split('.')[1] == 'parquet':
        #         file_format = 'parquet'
        #         header = 'NA'
        #         inferSchema = 'NA'

        #df_city = load_files(spark=spark, file_dir = file_dir, file_format= file_format, header = header, inferSchema=inferSchema)
        df_city = load_files(spark=spark, file_dir = "hdfs://localhost:9000/datalake/raw_layer/us_cities_dimension.parquet", inferSchema='NA', file_format='parquet', header='NA')

        # Load the Prescriber Fact File
        # for file in os.listdir(gav.staging_fact):
        #     print("File is " + file)
        #     file_dir = gav.staging_fact + '/' + file
        #     print(file_dir)
        #
        #     if file.split('.')[1] == 'csv':
        #         file_format = 'csv'
        #         header = gav.header
        #         inferSchema = gav.inferScehma
        #     elif file.split('.')[1] == 'parquet':
        #         file_format = 'parquet'
        #         header = 'NA'
        #         inferSchema = 'NA'
        #
        # df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
        #                      inferSchema=inferSchema)
        df_fact = load_files(spark=spark, file_dir = "hdfs://localhost:9000/datalake/raw_layer/USA_Presc_Medicare_Data_2021.csv", inferSchema=inferScehma, header=header, file_format="csv")


        #Validate run_data_ingest script for city Dimension & Prescriber Fact dataframe
        df_count(df_city,'df_city')
        df_top10_rec(df_city,'df_city')

        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact,'df_fact')

        #Initiate presc_run_data_preprocessing Script
        #Perform data Cleaning Operations for df_city and df_fact
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)

        # Validation for df_city and df_fact
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')


        # Initiate presc_run_data_transform Script
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        # Validation for df_city_final
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')

        # df_city_final.write.csv("../final/city_final.csv")
        # df_presc_final.write.csv("../final/presc_final.csv")

        # Save df_city_final as CSV
        # df_city_final.write.mode("overwrite").csv("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/city.csv")
        #
        # # Save df_presc_final as CSV
        # df_presc_final.write.mode("overwrite").csv("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/presc.csv")
        #
        df_city_final.show()
        df_presc_final.show()

        # df_city_final.write.parquet("hdfs://localhost:9000/datalake/staging_layer/test/")

        #writing to local
        df_city_final.write.csv("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/city/" ,header=True, mode="overwrite")
        df_presc_final.write.csv("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/presc/" ,header=True, mode="overwrite")


        #writing to hadoop
        df_city_final.write.csv("hdfs://localhost:9000/datalake/publish_layer/city/",
                                header=True, mode="overwrite")
        df_presc_final.write.csv("hdfs://localhost:9000/datalake/publish_layer/presc",
                                 header=True, mode="overwrite")


        logging.info("presc_run_pipeline.py is completed")

    except Exception as exp:
        logging.error("Error occurred in the main() method " + str(exp), exc_info=True)
        sys.exit(1)



if __name__ == '__main__':
    logging.info("run_presc_pipeline is started..")
    main()