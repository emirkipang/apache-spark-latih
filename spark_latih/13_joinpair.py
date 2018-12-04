from pyspark import SparkContext, SparkConf
import re

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ages = sc.parallelize([("Hery", 29),("Rangga", 19),("Wahyu", 35)])
    address = sc.parallelize([("Hery", "Bandung"),("Adeng", "Lamongan"),("Cindy", "Jakarta")])

    join = ages.join(address)
    join.coalesce(1).saveAsTextFile("../out/ages_address_join.txt")

    join = ages.leftOuterJoin(address)
    join.coalesce(1).saveAsTextFile("../out/ages_address_leftjoin.txt")

    join = ages.rightOuterJoin(address)
    join.coalesce(1).saveAsTextFile("../out/ages_address_rightjoin.txt")

    join = ages.fullOuterJoin(address)
    join.coalesce(1).saveAsTextFile("../out/ages_address_fulljoin.txt")