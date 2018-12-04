import sys
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

sys.path.insert(0, '.')

def filterResponseCanada(response):
    splits = Utils.COMMA_DELIMITER.split(response)
    total.add(1) #total dari data index ke 1, bisa mana aja sebenernya
    missingSalaryMidPoint.add(14)
    return splits[2] == "Canada"

if __name__ == '__main__':
    conf = SparkConf()
    #conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf = conf)

    total = sc.accumulator(0)
    missingSalaryMidPoint = sc.accumulator(0)
    responseRDD = sc.textFile("../in/2016-stack-overflow-survey-responses.csv")

    responCanada = responseRDD.filter(filterResponseCanada)

    print("Jumlah responden dari canada {}".format(responCanada.count()))
    print("Total Missing salary midpoint canada {}".format(re))
    # total semua data, gaji yg hilang dari canada