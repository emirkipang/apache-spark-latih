from os.path import basename

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# opener = urllib2.build_opener()
# f = opener.open(req)
# json = json.loads(f.read())
# print json['ci']

def applyQuery(path):
    with open(path, 'r') as myfile:
        data = myfile.read()
    spark.sql(data).createOrReplaceTempView(str.replace(basename(path),".sql",""))

def makeParts(data):
    #req = urllib2.Request("http://10.250.200.217:8001/?oprid=getAll&msisdn=6282293718474&show=json")
    rows = data.split("~")
    if(len(rows) == 46):
        return rows

def createScheme(path, delimiter, schemaString, name):
    lines = sc.textFile(path)
    # parts = lines.map(lambda l: l.split(delimiter))
    parts = lines.map(makeParts)
    data = parts.map(lambda p: tuple(p))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    spark.createDataFrame(data, schema).createOrReplaceTempView(name)

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext

    # ########## 0. Define args ##########
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--source")
    # parser.add_argument("--query")
    # parser.add_argument("--current_hive_before")
    # parser.add_argument("--current_hive_after")
    # parser.add_argument("--output")
    #
    # args = parser.parse_args()
    #
    # ########## 1. Init path ##########
    # # src
    # path_src = args.source
    # # ref
    # path_current_hive_before = args.current_hive_before
    # path_current_hive_after = args.current_hive_after
    # # query
    # path_query_trans_record = args.query + "/trans_record.sql"
    # # output
    # path_output = args.output
    #
    # ########## 2. Load data to source table ##########
    # # trans_record
    # createScheme(path=path_src,
    #              delimiter="~",
    #              schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE DEBIT_MSISDN CREDIT_MSISDN",
    #              name = "trans_record_src")
    #
    # # # trans_record before
    # # createScheme(path=path_current_hive_before,
    # #              delimiter="|",
    # #              schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE LAC_DEBIT CI_DEBIT LAC_CREDIT CI_CREDIT",
    # #              name="trans_record_before")
    # #
    # # # trans_record after
    # # createScheme(path=path_current_hive_after,
    # #              delimiter="|",
    # #              schemaString="ORDERID TRANS_STATUS TRANS_INITATE_TIME TRANS_END_TIME DEBIT_PARTY_ID DEBIT_PARTY_TYPE DEBIT_PARTY_ACCOUNT DEBIT_ACCOUNT_TYPE DEBIT_PARTY_MNEMONIC CREDIT_PARTY_ID CREDIT_PARTY_TYPE CREDIT_PARTY_ACCOUNT CREDIT_ACCOUNT_TYPE CREDIT_PARTY_MNEMONIC EXPIRED_TIME REQUEST_AMOUNT REQUEST_CURRENCY EXCHANGE_RATE ORG_AMOUNT ACTUAL_AMOUNT FEE COMMISSION TAX ACCOUNT_UNIT_TYPE CURRENCY IS_REVERSED REMARK IS_PARTIAL_REVERSED IS_REVERSING CHECKER_ID REASON_TYPE LAST_UPDATED_TIME VERSION LOAD_DATA_TS ACCUMULATOR_UPDATE ACCUMULATOR_REVERSAL CHG_RATING_DETAILS BANK_CARD_ID BANK_ACCOUNT_NUMBER BANK_ACCOUNT_NAME FI_ACCOUNT_INFO DISCOUNT_AMOUNT REDEEMED_POINT_AMOUNT REDEEMED_POINT_TYPE LAC_DEBIT CI_DEBIT LAC_CREDIT CI_CREDIT",
    # #              name="trans_record_after")
    #
    # ########## 3. Apply Query to agg table ##########
    # # Create trans_record
    # # applyQuery(path_query_trans_record)
    #
    # ########## 4. Output ##########
    # spark.sql("select * from trans_record_src").coalesce(1).write.csv(path_output)

