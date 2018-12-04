from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]")
        #.setAppName("nasa log intersection")

    sc = SparkContext(conf=conf)

    juliLog = sc.textFile("../in/nasa_19950701.tsv")
    agtLog  = sc.textFile("../in/nasa_19950801.tsv")

    juliHost = juliLog.map(lambda line: line.split("\t")[0])
    agtHost  = agtLog.map(lambda line: line.split("\t")[0])

    irisan  = juliHost.intersection(agtHost)

    irisanFinal = irisan.filter(lambda line : line != "host")
    #print(irisanFinal.collect())
    irisanFinal.saveAsTextFile("../out/nasa_log_juli_agt.csv")


# Notes
# - re.sub('[^a-zA-Z0-9 .]', '', line.lower()) => replace special karakter kecuali a-z & A-Z & 0-9 & " " & "."