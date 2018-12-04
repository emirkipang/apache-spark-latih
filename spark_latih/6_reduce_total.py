from pyspark import SparkContext, SparkConf

#runningnya pake terminal
if __name__ == '__main__':
    conf = SparkConf()\
        .setAppName("reduce")\
        .setMaster("local[2]")     \
        .set("spark.hadoop.validateOutputSpecs", "false")


    sc = SparkContext(conf=conf)

    lines = sc.textFile("in/prime_nums.text")
    numbers = lines.flatMap(lambda line : line.split("\t"))

    validNumbers = numbers.map(lambda number : int(number))
    total = validNumbers.reduce(lambda x,y : x+y)

    print("Total : {}".format(total))



# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."