from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/test_output/")
df_2 = spark.read.parquet("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/test_output_2/")

df.show()
df_2.show()
df.printSchema()
df_2.printSchema()

result = df.union(df_2)
result.show()
result.printSchema()

result.write.parquet("/home/anmol/PycharmProjects/pipeline/pipeline/src/main/python/final/test_output_3/")
