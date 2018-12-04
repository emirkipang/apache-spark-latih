from pyspark import SparkContext, SparkConf

#runningnya pake terminal
if __name__ == '__main__':
    conf = SparkConf()\
        .setAppName("pair rdd")\
        .setMaster("local[2]")     \
        .set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    tuples = [("herul",20),("wahyu",25),("cindy",18),("hery",40)]

    # tuples to pairRDD
    pairRDD = sc.parallelize(tuples)

    pairRDD.coalesce(1).saveAsTextFile("out/pairRddFromTuple.txt")