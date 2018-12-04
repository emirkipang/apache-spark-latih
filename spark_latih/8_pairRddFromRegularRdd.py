from pyspark import SparkContext, SparkConf

#runningnya pake terminal
if __name__ == '__main__':
    conf = SparkConf()\
        .setAppName("pair rdd")\
        .setMaster("local[2]")     \
        .set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    list = ["herul 20","wahyu 25","cindy 18","hery 40"]

    # tuples to pairRDD
    regRDD = sc.parallelize(list)
    pairRDD = regRDD.map(lambda x : (x.split(" ")[0], x.split(" ")[1]) )

    pairRDD.coalesce(1).saveAsTextFile("out/pairRddFromRegularRdd.txt")