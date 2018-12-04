from pyspark import SparkContext, SparkConf

if __name__ == '__main__' :
    conf = SparkConf().setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("../in/word_count.text")
    save = lines.flatMap(lambda x: (x.split(" ")))

    exit()
    save = save.map(lambda x: (x.lower()))
    save2 = save.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda a: a[1],ascending=False)


    #print(save2.collect())
    save2.coalesce(1).saveAsTextFile("../out/pairRDD_from_wordCount.txt")