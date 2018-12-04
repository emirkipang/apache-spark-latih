from pyspark.sql import SparkSession
from pyspark.sql import functions as fs

# MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status

if __name__ == "__main__":
    session = SparkSession.builder.master("local[2]").getOrCreate()
    # .appName("Survey stack overflow")\

    dataFrameReader = session.read

    print("========== Init uk-makerspaces-identifiable-data.csv ==========")
    responsesLeft = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("../in/uk-makerspaces-identifiable-data.csv")
    responColumnLeft = responsesLeft.select("Name of makerspace" ,"Postcode")


    print("========== Init uk-postcode.csv ==========")
    responsesRight = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("../in/uk-postcode.csv") \
        .withColumn("PostCode2", fs.concat_ws("", fs.col("Postcode"),fs.lit(" ")))
    responColumnRight = responsesRight.select("PostCode2","Region")


    joinResult = responColumnLeft.join(responColumnRight, responColumnLeft["Postcode"].startswith(responColumnRight["PostCode2"]),"left_outer")


    # joinResult.groupBy("Region").count().show()

    print("========== tugas tampilkan Name of makerspace, Postcode, Region diurut berdasarkan region ==========")
    joinResult.select("Name of makerspace", "Postcode", "Region").orderBy("Region", ascending = True).show()
