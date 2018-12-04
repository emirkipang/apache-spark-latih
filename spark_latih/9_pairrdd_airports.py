from pyspark import SparkContext, SparkConf
import re

def pairRDD(line):
    x = line.split(",")
    return x[1],x[3].upper()

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ### Get file
    linesRDD = sc.textFile("../in/airports.text")

    pairRDD = linesRDD.map(pairRDD)
    pairRDD = pairRDD.filter(lambda x : x[1] == "\"UNITED STATES\"")

    pairRDD.coalesce(1).saveAsTextFile("../out/airports_pairrdd.txt")


# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."
("emir","septi","ansori",1,100,200)