from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
	conf = SparkConf()
    #conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf = conf)

    # internal element
    inputWords = ["spark","hadoop","spark","hive","pig","cassandra","hadoop"]
    wordRdd = sc.parallelize(inputWords)

    wordCount = wordRdd.count()
    wordCountByValue = wordRdd.countByValue()

    print(wordCount)
    print(wordCountByValue)

    for word, count in wordCountByValue.items():
        print("{} : {} ".format(word,count))

    #print(words)

