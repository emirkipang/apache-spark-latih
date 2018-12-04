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
    rows = "~".join(str(d) for d in data)
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
    conf.setAppName("Digipos nsb")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)


    ########## 0. Define args ##########
    parser = argparse.ArgumentParser()
    parser.add_argument("--source")
    parser.add_argument("--query")
    parser.add_argument("--current_hive_before")
    parser.add_argument("--current_hive_after")
    parser.add_argument("--output")

    args = parser.parse_args()

    ########## 1. Init path ##########
    # src
    path_src = args.source
    # ref
    path_current_hive_before = args.current_hive_before
    path_current_hive_after = args.current_hive_after
    # query
    path_query_digipos_nsb = args.query + "/digipos_nsb.sql"
    # output
    path_output = args.output

    ########## 2. Load data to source table ##########
    # digipos_nsb
    createScheme(path=path_src,
                 delimiter="~",
                 schemaString="REQUEST_ID TRX_NO TRX_ID ORDER_NO A_NUMBER B_NUMBER OUTLET_ID ACTIVATION_STATUS ACTIVATION_TIME DENOM_RECHARGE RECHARGE_TYPE RECHARGE_STATUS RECHARGE_TIME PRODUCT PRICE PACKAGE_STATUS PACKAGE_TIME STATUS STATUS_DESC CREATED_AT CREATED_BY UPDATED_AT UPDATED_BY CASHBACK_STATUS CASHBACK_DESC ZONE",
                 name = "digipos_nsb_src")

    # digipos_nsb before
    createScheme(path=path_current_hive_before,
                 delimiter="~",
                 schemaString="REQUEST_ID TRX_NO TRX_ID ORDER_NO A_NUMBER B_NUMBER OUTLET_ID ACTIVATION_STATUS ACTIVATION_TIME DENOM_RECHARGE RECHARGE_TYPE RECHARGE_STATUS RECHARGE_TIME PRODUCT PRICE PACKAGE_STATUS PACKAGE_TIME STATUS STATUS_DESC CREATED_AT CREATED_BY UPDATED_AT UPDATED_BY CASHBACK_STATUS CASHBACK_DESC ZONE",
                 name="digipos_nsb_before")

    # digipos_nsb after
    createScheme(path=path_current_hive_after,
                 delimiter="~",
                 schemaString="REQUEST_ID TRX_NO TRX_ID ORDER_NO A_NUMBER B_NUMBER OUTLET_ID ACTIVATION_STATUS ACTIVATION_TIME DENOM_RECHARGE RECHARGE_TYPE RECHARGE_STATUS RECHARGE_TIME PRODUCT PRICE PACKAGE_STATUS PACKAGE_TIME STATUS STATUS_DESC CREATED_AT CREATED_BY UPDATED_AT UPDATED_BY CASHBACK_STATUS CASHBACK_DESC ZONE",
                 name="digipos_nsb_after")

    ######### 3. Apply Query to agg table ##########
    # Create digipos_nsb
    applyQuery(path_query_digipos_nsb)

    ########## 4. Output ##########
    sqlContext.sql("select * from digipos_nsb").map(toCSVLine).saveAsTextFile(path_output)


