from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('spark proj').getOrCreate()

def create_dataframe(data,ishead):
    df = spark.read.csv(data, header=ishead, inferSchema=True)
    return df








