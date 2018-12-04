from pyspark import SparkContext, SparkConf
import re

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ### Get file
    lines = sc.textFile("../in/RealEstate.csv")
    pairRDD = lines.filter(lambda line: line.split(",")[7] == "Foreclosure")
    pairRDD = pairRDD.map(lambda line : ((line.split(",")[7],line.split(",")[1]), eval(line.split(",")[3])))

    pairRDD = pairRDD.reduceByKey(lambda x,y: x+y)
    pairRDD = pairRDD.map(lambda line : (line[0][1], line[1], line[0][0]))

    print(pairRDD.collect())

    exit()
    # pairRDD = pairRDD.map(lambda line : (line,1))
    # pairRDD = pairRDD.reduceByKey(lambda x,y : x+y)
    # pairRDD = pairRDD.sortBy(lambda x : x[1], ascending=False)
    #
    # pairRDD.coalesce(1).saveAsTextFile("../out/pairdd_word_count.txt")


# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."

# MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status
# 132842,Arroyo Grande,795000.00,3,3,2371,335.30,Short Sale
# 134364,Paso Robles,399000.00,4,3,2818,141.59,Short Sale
# 135141,Paso Robles,545000.00,4,3,3032,179.75,Short Sale
# 135712,Morro Bay,909000.00,4,4,3540,256.78,Short Sale