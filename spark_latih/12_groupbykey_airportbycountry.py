from pyspark import SparkContext, SparkConf
from commons.Utils import Utils
import sys

sys.path.insert(0,'.')

if __name__ == '__main__':
    conf = SparkConf()
    # conf.setAppName("Basic Action")
    conf.setMaster("local[*]")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=conf)

    ### Get file
    lines = sc.textFile("../in/airports.text")

    countryAndNamePair = lines.map(lambda airport: (Utils.COMMA_DELIMITER.split(airport)[3],Utils.COMMA_DELIMITER.split(airport)[1]))

    airpotsByCountry = countryAndNamePair.groupByKey()

    # print(airpotsByCountry.collect())

    for country, airport in airpotsByCountry.collectAsMap().items():
        print("{} {}".format(country, list(airport)))

# 135141,Paso Robles,545000.00,4,3,3032,179.75,Short Sale
# 135712,Morro Bay,909000.00,4,4,3540,256.78,Short Sale