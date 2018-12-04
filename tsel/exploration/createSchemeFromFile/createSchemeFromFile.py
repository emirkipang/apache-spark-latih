from os.path import basename

import argparse
from pyspark import SparkConf, SparkContext
from pyspark.shell import sc
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *



def applyQuery(path):
    with open(path, 'r') as myfile:
        data = myfile.read()
    sqlContext.sql(data).registerTempTable(str.replace(basename(path),".sql",""))

def toCSVLine(data):
    rows = "|".join(str(d) for d in data)
    return rows

def createScheme(path, delimiter, schemaString, name):
    lines = sc.textFile(path)
    parts = lines.map(lambda l: l.split(delimiter))
    data = parts.map(lambda p: tuple(p))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    sqlContext.createDataFrame(data, schema).registerTempTable(name)

if __name__ == '__main__':
    sc.stop()

    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.setAppName("test create scheme from file")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    ########## 1. Init path ##########
    # src
    path_src = "file:///home/hnat_tcash/sample/test.csv"
    path_query_digipos_package = "/home/hnat_tcash/sample/test.sql"
    ########## 2. Load data to source table ##########
    # digipos_package
    createScheme(path=path_src,
                 delimiter="|",
                 schemaString="col1",
                 name = "myTable")


    ######### 3. Apply Query to agg table ##########
    # Create digipos_package
    applyQuery(path_query_digipos_package)

    ########## 4. Output ##########
    sqlContext.sql("select * from myTable").map(toCSVLine).saveAsTextFile("file:///home/hnat_tcash/sample/test.out")

#
# from pyspark import SparkConf, SparkContext
# from pyspark.shell import sc
# from pyspark.sql import SQLContext, Row
# from pyspark.sql.types import *
#
# sc.stop()
#
# conf = SparkConf()
# conf.set("spark.hadoop.validateOutputSpecs", "false")
# conf.setAppName("test create scheme from file")
#
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
# lines = sc.textFile("test.csv")
# parts = lines.map(lambda l: l.split("|"))
# people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
#
# # Infer the schema, and register the DataFrame as a table.
# schemaPeople = sqlContext.createDataFrame(people)
# schemaPeople.registerTempTable("myTable")
#
# # SQL can be run over DataFrames that have been registered as a table.
# teenagers = sqlContext.sql("SELECT name FROM myTable")
#
# # The results of SQL queries are RDDs and support all the normal RDD operations.
# teenNames = teenagers.map(lambda p: "Name: " + p.name)
# for teenName in teenNames.collect():
#   print(teenName)
