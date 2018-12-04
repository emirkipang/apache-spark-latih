import sys
from pyspark import SparkContext, SparkConf

sys.path.insert(0, '.')

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")

sc = SparkContext("local[*]", "Ambil data helo")

# inputangka = list(range(1,10))
# angkaRDD = sc.parallelize(inputangka)
# angkaRDD.collect()

if __name__ == '__main__':
    ##### FlatMap #####
    linesRDD = sc.textFile("../in/hello.txt")
    words = linesRDD.flatMap(lambda line : line.split(" "))
    # print(words.collect())
    print(words.take(2))
    words.saveAsTextFile("../out/hellosplit_flatmap.txt")

##### Map #####
# words = linesRDD.map(lambda line : line.split(" "))
# # print(words.collect())
# print(words.take(2))
# words.saveAsTextFile("../out/hellosplit_map.txt")

