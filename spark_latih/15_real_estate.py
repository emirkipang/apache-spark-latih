from pyspark.sql import SparkSession
from pyspark.sql import  functions as func

# MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status

if __name__ == "__main__":
    session = SparkSession.builder.master("local[2]").getOrCreate()
    # .appName("Survey stack overflow")\

    dataFrameReader = session.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("../in/RealEstate.csv")

    print("========== Tampilkan Schema ==========")
    # responses.printSchema()

    responColumn = responses.select("Location", "Price SQ Ft")

    print("========== Group by Location ==========")
    responColumn.groupBy("Location").count() \
        .show()

    print("========== Group by Location with avg ==========")
    responColumn.groupBy("Location")\
        .avg("Price SQ Ft")\
        .withColumnRenamed("avg(Price SQ Ft)","Average Price")\
        .orderBy("avg(Price SQ Ft)", ascending = False) \
        .show()
