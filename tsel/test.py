from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext