from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':

    sc = SparkContext()
    ssc = StreamingContext(sc, 1)

    lines = ssc.textFileStream("in/hello.txt")
    counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda kata: (kata,1))\
        .reduceByKey(lambda a,b : a+b)

    counts.pprint()

    ssc.start()