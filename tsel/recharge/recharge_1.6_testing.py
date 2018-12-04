from os.path import basename

from pyspark import SparkConf, SparkContext
from pyspark.shell import sc
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from datetime import datetime

def applyQuery(path):
    with open(path, 'r') as myfile:
        data = myfile.read()
    sqlContext.sql(data).registerTempTable(str.replace(basename(path),".sql",""))

def toCSVLine(data):
    return '|'.join(str(d) for d in data)

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
    conf.setAppName("Recharge Summary")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    base_query = "/data01/hnat_qsr/recharge"

    ########## 1. Init path ##########
    #datenow = datetime.now().strftime('%Y%m%d')
    datenow = '20181023'

    # src
    path_src_mkios = "file://" + base_query + "/mkios/test/prc/*"
    path_src_urp = "file://" + base_query + "/urp/test/prc/*"

    # ref
    path_ref_lacima_3g = "file://" + base_query + "/ref/laccima/v_par_ref_lacima.csv"
    path_ref_lacima_4g = "file://" + base_query + "/ref/laccima/v_par_ref_lacima_4g.csv"
    #path_ref_mostdom_lacima = "file://"  + base_query +  "/ref/mostdom/mostdom_lacima_v2.csv"
    path_ref_mostdom_lacima = "file://" + base_query + "/ref/solusi/ADHOC_DUMP_MOSTDOM_LACCI.txt"
    path_ref_splitcode_mkios = "file://"  + base_query +  "/ref/splitcode/split_code_mkios.csv"
    path_ref_splitcode_urp = "file://" + base_query + "/ref/splitcode/split_code_urp.csv"
    path_ref_digipos = "file://"  + base_query +  "/ref/digipos/" + datenow + "/*"

    # query
    path_query_lacima = base_query + "/query/lacima.sql"
    path_query_recharge = base_query + "/query/recharge_test.sql"
    path_query_output = base_query + "/query/output_test.sql"

    # output
    path_output = "file://" + base_query + "/test/out/recharge_summary_temp"

    ########## 2. Load data to source table ##########
    # mkios
    createScheme(path=path_src_mkios,
                 delimiter="|",
                 schemaString="Serial_No_in_the_system Region_ID Hand_phone_No_of_a_Dealer Reserved_String Initiator_Template_ID Dealers_name Brand_type Users_brand_type Price MSISDN Reserved_String1 Recharge_Date SerialNo_of_a_rechargeable_card Response_state Transaction_status Reserved_String2 Error_message Access_type Dealer_actual_decrease_money Acceptor_actual_increase_money Reserved Reserved2 ICCID Balance_of_stock_before_adjustor Reserved3 Balance_after_recharge Reserved4 LACCI_RS Reserved_String3 Reserved_String4 Reserved5 Account_Index Old_Balance New_Balance Reserved6 Reserved_String5 Reserved7 Reserved8 Reserved9 Upstream_MSISDN Upstream_Name LACCI_subscriber Reserved_String6 recharge_type SEPID GWNo Active_date_before Active_date_after Reseller_type Cross_region_identifier Unidentified_16 Unidentified_17 Unidentified_18 Unidentified_19 Unidentified_20 Unidentified_21 AD_MSISDN Unidentified_22 Unidentified_23 Unidentified_24 Unidentified_25 Unidentified_26 Unidentified_27 Unidentified_28 Unidentified_29 Unidentified_30 Unidentified_31 Unidentified_32 Unidentified_33 Unidentified_34 Unidentified_35 Unidentified_36 Node_Flag Unidentified_38 Unidentified_39 Unidentified_40",
                 name = "mkios")

    # urp
    createScheme(path=path_src_urp,
                 delimiter="/",
                 schemaString="SNE BrandID Credit Tax CreditPeriod CreditExpiryDate OperState OperStateDate CreateDate SenderMedium SenderUserId AccountId subAccountId RecipientUserId OperError OperErrorInfo SepID SubsType VoucherType StockType ProcessingCode Cati Cai Can LacCi ProcessTime TrxIdPartner TrxIdTsel Node IMEI PartnerTransmissionDate",
                 name = "urp")

    # laccima 3G
    createScheme(path=path_ref_lacima_3g,
                 delimiter="|",
                 schemaString="lac cell_id site_name vendor site_id ne_id bsc_name msc_name node longitude latitude azimuth regional_network propinsi kabupaten kecamatan kelurahan regional_channel branch subbranch address sector cluster mitra_ad cgi_prepaid cgi_postpaid ocs_cluster ocs_zone type_lacci on_air_date x",
                 name = "lacima_3g")

    # laccima 4G
    createScheme(path=path_ref_lacima_4g,
                 delimiter="|",
                 schemaString="lac cell_id site_name vendor site_id ne_id bsc_name msc_name node longitude latitude azimuth regional_network propinsi kabupaten kecamatan kelurahan regional_channel branch subbranch address sector cluster mitra_ad cgi_prepaid cgi_postpaid ocs_cluster ocs_zone type_lacci on_air_date x",
                 name = "lacima_4g")

    # mostdom
    # createScheme(path=path_ref_mostdom_lacima,
    #              delimiter="|",
    #              schemaString="msisdn area region branch subbranch cluster",
    #              name="mostdom_lacima")

    createScheme(path=path_ref_mostdom_lacima,
                 delimiter="|",
                 schemaString="trx_date msisdn lacci_id lac ci node area region branch subbranch cluster kabupaten",
                 name="mostdom_lacima")

    # splitcode mkios
    createScheme(path=path_ref_splitcode_mkios,
                 delimiter="|",
                 schemaString="type channel",
                 name="splitcode_mkios")

    # splitcode urp
    createScheme(path=path_ref_splitcode_urp,
                 delimiter="|",
                 schemaString="type channel",
                 name="splitcode_urp")


    # digipos
    createScheme(path=path_ref_digipos,
                 delimiter="|",
                 schemaString="date anumber bnumber",
                 name="digipos")

    ########## 3. Apply Query to agg table ##########
    # Create laccima
    applyQuery(path_query_lacima)

    # Create recharge
    applyQuery(path_query_recharge)

    # Create output
    applyQuery(path_query_output)

    ########## 4. Output ##########
    #sqlContext.sql("select * from recharge").show()
    #sqlContext.sql("select * from mostdom_lacima").show()

    sqlContext.sql("select * from output_test").map(toCSVLine).coalesce(10).saveAsTextFile(path_output)

