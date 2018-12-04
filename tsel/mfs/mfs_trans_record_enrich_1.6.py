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

def getLacci(msisdn):
    import json
    import urllib2

    if(msisdn.strip() != ""):
        req = urllib2.Request("http://10.250.200.217:8001/?oprid=getAll&msisdn=" + msisdn + "&show=json")
        opener = urllib2.build_opener()
        f = opener.open(req)
        json = json.loads(f.read())

        if json['ResultCode'] == '0':
            return json['ci']+ "~" +json['product_id']+ "~" +json['kabupaten']+ "~" +json['area']+ "~" +json['osvendor']+ "~" +json['ResultDesc']+ "~" +json['regional']+ "~" +json['sposname']+ "~" +json['lac']+ "~" +json['propinsi']+ "~" +json['branch']+ "~" +json['ttype']+ "~" +json['imei']+ "~" +json['tvendor']+ "~" +json['ResultCode']+ "~" +json['osversion']+ "~" +json['imsi']
        else:
            return "0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"
    else:
        return "0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"+ "~" +"0"


def toCSVLine(data):
    debit_msisdn = data.asDict()["DEBIT_MSISDN"]
    credit_msisdn = data.asDict()["CREDIT_MSISDN"]
    lacci_debit_credit = getLacci(debit_msisdn) + "~" + getLacci(credit_msisdn)
    rows = "~".join(str(d) for d in data)
    return rows + "~" + lacci_debit_credit

def makeParts(data):
    rows = data.split("~")
    if(len(rows) == 46):
        return rows

def createScheme(path, delimiter, schemaString, name, type=""):
    lines = sc.textFile(path)

    if type == "src":
        parts = lines.map(makeParts)
    else:
        parts = lines.map(lambda l: l.split(delimiter))

    data = parts.map(lambda p: tuple(p.strip()))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    sqlContext.createDataFrame(data, schema).registerTempTable(name)


if __name__ == '__main__':
    sc.stop()

    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.setAppName("Trans Record Enrichment")

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
    path_query_trans_record = args.query + "/trans_record.sql"
    # output
    path_output = args.output

    ########## 2. Load data to source table ##########
    # trans_record
    createScheme(path=path_src,
                 delimiter="~",
                 schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE DEBIT_MSISDN CREDIT_MSISDN",
                 name = "trans_record_src",
                 type = "src")

    # trans_record before
    createScheme(path=path_current_hive_before,
                 delimiter="~",
                 schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE ci_debit product_id_debit kabupaten_debit area_debit osvendor_debit ResultDesc_debit regional_debit sposname_debit lac_debit propinsi_debit branch_debit ttype_debit imei_debit tvendor_debit ResultCode_debit osversion_debit imsi_debit ci_kredit product_id_kredit kabupaten_kredit area_kredit osvendor_kredit ResultDesc_kredit regional_kredit sposname_kredit lac_kredit propinsi_kredit branch_kredit ttype_kredit imei_kredit tvendor_kredit ResultCode_kredit osversion_kredit imsi_kredit period hour",
                 name="trans_record_before")

    # trans_record after
    createScheme(path=path_current_hive_after,
                 delimiter="~",
                 schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE ci_debit product_id_debit kabupaten_debit area_debit osvendor_debit ResultDesc_debit regional_debit sposname_debit lac_debit propinsi_debit branch_debit ttype_debit imei_debit tvendor_debit ResultCode_debit osversion_debit imsi_debit ci_kredit product_id_kredit kabupaten_kredit area_kredit osvendor_kredit ResultDesc_kredit regional_kredit sposname_kredit lac_kredit propinsi_kredit branch_kredit ttype_kredit imei_kredit tvendor_kredit ResultCode_kredit osversion_kredit imsi_kredit period hour",
                 name="trans_record_after")

    ######### 3. Apply Query to agg table ##########
    # Create trans_record
    applyQuery(path_query_trans_record)

    ########## 4. Output ##########
    sqlContext.sql("select * from trans_record").map(toCSVLine).saveAsTextFile(path_output)

