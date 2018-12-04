from pyspark import SparkContext, SparkConf
import re

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ### Get file
    linesRDD = sc.textFile("../in/airports.text")

    ### without filter
    # i = 0
    # lines = linesRDD.flatMap(lambda line : line.split("\n"))
    # for line in lines.collect():
    #     words = line.split(",")
    #     negara = words[3]
    #
    #     if negara == "\"United States\"":
    #         print(line)
    #         i+=1
    # print(i)

    ### by filter
    lines = linesRDD.filter(lambda line : line.split(",")[3] == "\"United States\"")
    lines = lines.map(lambda line : line.split(",")[1]+","+ line.split(",")[2] + "," + line.split(",")[3])
    lines.saveAsTextFile("../out/airports_map.txt")

    # for line in lines.collect():
    #     words = line.split(",")
    #     print(words[1])
    # print(lines.count())


# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."