from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSaveCSV").getOrCreate()

data = [("A", 1), ("B", 2), ("C", 3)]
columns = ["Name", "Value"]

df = spark.createDataFrame(data, columns)

try:
    df.write.mode("overwrite").option("header", "true").parquet("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/test_output_2")
    print("CSV saved successfully")
except Exception as e:
    print(f"Error saving CSV: {e}")
