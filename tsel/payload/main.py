from pyspark import SparkContext, SparkConf
from pyspark.shell import sc


def toCSVLine(data):
    return '|'.join(str(d) for d in data)

def getNodeType(AccessType):
    if(AccessType == "2"):
        return "2G"
    elif (AccessType == "1"):
        return "3G"
    elif (AccessType == "6"):
        return "4G"

def getLac(cgi, sai, ecgi):
    lac = ""
    if(cgi == ""):
        lac = cgi[5:]
        return "2G"
    elif (sai == ""):
        return "3G"
    elif (ecgi == ""):
        return "4G"

def pairRDD(line):
    x = line.split(",")

    ### Init
    trigger_type = x[0]
    time = x[1]
    cgi = x[10]
    SAI = x[11]
    AccessType = x[17]
    QuotaUsage = x[31]
    ECGI = x[46]

    ### Body
    nodeType = getNodeType(AccessType)


    return x[0], x[1], x[10], x[11], x[17], x[31], x[46]


### Init
conf = SparkConf()
sc.stop()

conf.setAppName("Payload")
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(conf=conf)

# Get file
linesRDD = sc.textFile("file:///home/hnat_qsr/payload/sample/1/*")

pairRDD = linesRDD.map(pairRDD)
pairRDD = pairRDD.filter(lambda x: (x[0] == "2" or x[0] == "3") and x[5] != '')

pairRDD.map(toCSVLine).coalesce(10).saveAsTextFile("file:///home/hnat_qsr/payload/out/1")

### Note
# No	Nama Kolom
# 1	TriggerType
# 2	Time
# 11	CGI
# 12	SAI
# 18	AccessType
# 32	QuotaUsage
# 47	ECGI
#
# herul tsel, [03.12.18 13:28]
# [Forwarded from Kusuma Adi]
# Access Type 2 is 2G
# Access Type 1 is 3G
# Access Type 6 is 4G
#
# herul tsel, [03.12.18 13:28]
# [Forwarded from Kusuma Adi]
# •  If CGI exists, then value network type is 2G
# •  If SAI exists, then value network type is 3G
# •  If ECGI exists, then value network type is 4G