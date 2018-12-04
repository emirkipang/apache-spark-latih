from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

def createScheme(path, delimiter, schemaString, name):
    lines = sc.textFile(path)
    parts = lines.map(lambda l: l.split(delimiter))
    data = parts.map(lambda p: tuple(p))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    spark.createDataFrame(data, schema).createOrReplaceTempView(name)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Recharge") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext

    # Load mkios
    createScheme(path="recharge/in/*.unl",
                 delimiter="|",
                 schemaString="Serial_No_in_the_system Region_ID Hand_phone_No_of_a_Dealer Reserved_String Initiator_Template_ID Dealers_name Brand_type Users_brand_type Price MSISDN Reserved_String1 Recharge_Date SerialNo_of_a_rechargeable_card Response_state Transaction_status Reserved_String2 Error_message Access_type Dealer_actual_decrease_money Acceptor_actual_increase_money Reserved Reserved2 ICCID Balance_of_stock_before_adjustor Reserved3 Balance_after_recharge Reserved4 LACCI_RS Reserved_String3 Reserved_String4 Reserved5 Account_Index Old_Balance New_Balance Reserved6 Reserved_String5 Reserved7 Reserved8 Reserved9 Upstream_MSISDN Upstream_Name LACCI_subscriber Reserved_String6 recharge_type SEPID GWNo Active_date_before Active_date_after Reseller_type Cross_region_identifier Unidentified_16 Unidentified_17 Unidentified_18 Unidentified_19 Unidentified_20 Unidentified_21 AD_MSISDN Unidentified_22 Unidentified_23 Unidentified_24 Unidentified_25 Unidentified_26 Unidentified_27 Unidentified_28 Unidentified_29 Unidentified_30 Unidentified_31 Unidentified_32 Unidentified_33 Unidentified_34 Unidentified_35 Unidentified_36 Node_Flag Unidentified_38 Unidentified_39 Unidentified_40",
                 name = "mkios")

    # Load urp
    createScheme(path="recharge/in/tran*",
                 delimiter="/",
                 schemaString="SNE BrandID Credit Tax CreditPeriod CreditExpiryDate OperState OperStateDate CreateDate SenderMedium SenderUserId AccountId subAccountId RecipientUserId OperError OperErrorInfo SepID SubsType VoucherType StockType ProcessingCode Cati Cai Can LacCi ProcessTime TrxIdPartner TrxIdTsel Node IMEI PartnerTransmissionDate",
                 name = "urp")

    # Load laccima 3G
    createScheme(path="recharge/in/v_par_ref_lacima.csv",
                 delimiter="|",
                 schemaString="lac cell_id site_name vendor site_id ne_id bsc_name msc_name node longitude latitude azimuth regional_network propinsi kabupaten kecamatan kelurahan regional_channel branch subbranch address sector cluster mitra_ad cgi_prepaid cgi_postpaid ocs_cluster ocs_zone type_lacci on_air_date x",
                 name = "lacima_3g")

    # Load laccima 4G
    createScheme(path="recharge/in/v_par_ref_lacima_4g.csv",
                 delimiter="|",
                 schemaString="lac cell_id site_name vendor site_id ne_id bsc_name msc_name node longitude latitude azimuth regional_network propinsi kabupaten kecamatan kelurahan regional_channel branch subbranch address sector cluster mitra_ad cgi_prepaid cgi_postpaid ocs_cluster ocs_zone type_lacci on_air_date x",
                 name = "lacima_4g")

    # Create laccima
    spark.sql(
        " SELECT"
        " CONCAT_WS('~',CONCAT(REPEAT('0',5 - LENGTH(lac)), lac),CONCAT(REPEAT('0',5 - LENGTH(cell_id)), cell_id)) as lacci,"
        " regional_channel as region,"
        " branch as branch,"
        " subbranch as subbranch,"
        " cluster as cluster,"
        " CASE"
        " WHEN regional_channel = 'SUMBAGSEL' OR regional_channel = 'SUMBAGUT' OR regional_channel = 'SUMBAGTENG' THEN 'AREA 1'"
        " WHEN regional_channel = 'WESTERN JABOTABEK' OR regional_channel = 'EASTERN JABOTABEK' OR regional_channel = 'CENTRAL JABOTABEK' OR regional_channel = 'JAWA BARAT' OR regional_channel = 'SAD REGIONAL' THEN 'AREA 2'"
        " WHEN regional_channel = 'JATENG-DIY' OR regional_channel = 'JATIM' OR regional_channel = 'BALI NUSRA' THEN 'AREA 3'"
        " WHEN regional_channel = 'KALIMANTAN' OR regional_channel = 'SULAWESI' OR regional_channel = 'PAPUA' OR regional_channel = 'MALUKU DAN PAPUA' THEN 'AREA 4'"
        " ELSE 'UNKNOWN'"
        " END as area,"
        " '3G' as node_type"
        " FROM lacima_3g"
        " UNION ALL"
        " SELECT"
        " CONCAT_WS('~',CONCAT(REPEAT('0',7 - LENGTH(lac)), lac),CONCAT(REPEAT('0',3 - LENGTH(cell_id)), cell_id)) as lacci,"
        " regional_channel as region,"
        " branch as branch,"
        " subbranch as subbranch,"
        " cluster as cluster,"
        " CASE"
        " WHEN regional_channel = 'SUMBAGSEL' OR regional_channel = 'SUMBAGUT' OR regional_channel = 'SUMBAGTENG' THEN 'AREA 1'"
        " WHEN regional_channel = 'WESTERN JABOTABEK' OR regional_channel = 'EASTERN JABOTABEK' OR regional_channel = 'CENTRAL JABOTABEK' OR regional_channel = 'JAWA BARAT' OR regional_channel = 'SAD REGIONAL' THEN 'AREA 2'"
        " WHEN regional_channel = 'JATENG-DIY' OR regional_channel = 'JATIM' OR regional_channel = 'BALI NUSRA' THEN 'AREA 3'"
        " WHEN regional_channel = 'KALIMANTAN' OR regional_channel = 'SULAWESI' OR regional_channel = 'PAPUA' OR regional_channel = 'MALUKU DAN PAPUA' THEN 'AREA 4'"
        " ELSE 'UNKNOWN'"
        " END as area,"
        " '4G' as node_type"
        " FROM lacima_4g"
    ).createOrReplaceTempView("lacima.sql")

    # create recharge
    spark.sql(""
    " SELECT"
        " substr(Recharge_Date,0,12) as date,"
        " 'MKIOS' as Channel,"
        " CONCAT_WS('~',substr(LACCI_RS,0,5),substr(LACCI_RS,6,5)) as LACCI_RS_3G,"
        " CONCAT_WS('~',substr(LACCI_RS,0,7),substr(LACCI_RS,8,3)) as LACCI_RS_4G,"
        " CONCAT_WS('~',substr(LACCI_subscriber,0,5),substr(LACCI_subscriber,6,5)) as LACCI_subscriber_3G,"
        " CONCAT_WS('~',substr(LACCI_subscriber,0,7),substr(LACCI_subscriber,8,3)) as LACCI_subscriber_4G,"
        " Acceptor_actual_increase_money as amount,"
        " '1' as trx"
    " FROM mkios WHERE Response_state = 'O00'"
    " UNION ALL"
    " SELECT"
        " substr(regexp_replace(regexp_replace(regexp_replace(CreateDate,':',''),'-',''),' ',''),0,12) as date,"
        " 'URP' as Channel,"
        " '00000~00000' as LACCI_RS_3G,"
        " '0000000~000' as LACCI_RS_4G,"
        " CONCAT_WS('~',substr(LacCi,0,5),substr(LacCi,6,5)) as LacCi_3G,"
        " CONCAT_WS('~',substr(LacCi,0,7),substr(LacCi,8,3)) as LacCi_4G,"
        " Credit as amount,"
        " '1' as trx"
    " FROM urp WHERE OperState = '61'").createOrReplaceTempView("recharge")

    spark.sql(" SELECT "
              " r.channel,"
              " r.date,"
              " COALESCE(l1.area, l2.area, 'UNKNOWN') as area,"
              " COALESCE(l1.region, l2.region, 'UNKNOWN') as region,"
              " COALESCE(l1.branch, l2.branch, 'UNKNOWN') as branch,"
              " COALESCE(l1.subbranch, l2.subbranch, 'UNKNOWN') as subbranch,"
              " COALESCE(l1.cluster, l2.cluster, 'UNKNOWN') as cluster,"
              " SUM(r.amount) as total_amount,"
              " SUM(r.trx) as total_trx"
              " FROM recharge r"
              " LEFT JOIN lacima.sql l1 ON r.LACCI_subscriber_3G = l1.lacci AND l1.node_type ='3G'"
              " LEFT JOIN lacima.sql l2 ON r.LACCI_subscriber_4G = l2.lacci AND l2.node_type ='4G'"
              " GROUP BY 1,2,3,4,5,6,7").show()

    # spark.sql("select * from recharge where channel = 'URP'").show()
