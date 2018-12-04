from pyspark import SparkContext, SparkConf
import re



if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ##### FlatMap #####
    linesRDD = sc.textFile("../in/word_count.text")

    words = linesRDD.flatMap(lambda line : re.sub('[^a-zA-Z0-9 \']', '', line.lower()).split(" "))
    wordCountByValue = words.countByValue()

    print(words.count())

    for word, count in wordCountByValue.items():
        print("{} : {} ".format(word, count))


# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."