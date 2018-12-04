from os.path import basename

from pyspark import SparkConf, SparkContext
from pyspark.shell import sc
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

def applyQuery(path):
    with open(path, 'r') as myfile:
        data = myfile.read()
    sqlContext.sql(data).registerTempTable(str.replace(basename(path),".sql",""))

def toCSVLine(data):
    return '|'.join(str(d) for d in data)

def createScheme_custom(path, delimiter, schemaString, name):
    lines = sc.textFile(path)
    parts = lines.map(lambda l: l.split(delimiter) if len(l.split(delimiter)) == 176 else ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,')
    data = parts.map(lambda p: tuple(p))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(delimiter)]
    schema = StructType(fields)
    sqlContext.createDataFrame(data, schema).registerTempTable(name)

def createScheme(path, delimiter, schemaString, name):
    lines = sc.textFile(path)
    parts = lines.map(lambda l: l.split(delimiter))
    data = parts.map(lambda p: tuple(p))
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(delimiter)]
    schema = StructType(fields)
    sqlContext.createDataFrame(data, schema).registerTempTable(name)

if __name__ == '__main__':
    sc.stop()

    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "16g")
    conf.setAppName("Payload Summary")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    ########## 1. Init path ##########
    # src
    path_src_upcc = "file:///data01/hnat_qsr/payload/prc/*"

    # ref
    path_ref_lacima_3g = "file:///data01/hnat_qsr/payload/ref/lacima/report_teradata_20181203.csv"
    path_ref_lacima_4g = "file:///data01/hnat_qsr/payload/ref/lacima/report_teradata_4G_20181203.csv"

    # query
    path_query_lacima = "/data01/hnat_qsr/payload/query/lacima.sql"
    path_query_output = "/data01/hnat_qsr/payload/query/output.sql"

    # output
    path_output = "file:///data01/hnat_qsr/payload/out/payload_summary_temp"

    ########## 2. Load data to source table ##########
    # upcc
    createScheme_custom(path=path_src_upcc,
                 delimiter=",",
                 schemaString="TriggerType,Time,SubscriberIdentifier,IMSI,MSISDN,IMEI,PaidType,Category,HomeServiceZone,VisitServiceZone,CGI,SAI,RAI,SGSNAddress,MCCMNC,RoamingStatus,RoamingRegion,AccessType,IPCanType,UEIP,APN,DownloadBandwidth,UploadBandwidth,ServicePackageName,ServiceName,RuleName,QuotaName,QuotaStatus,QuotaConsumption,QuotaBalance,QuotaRecharge,QuotaUsage,QuotaNextResetTime,AccountName,AccountStatus,AccountPrivilege,PersonalValue,AccountBalance,AccountConsumption,AccountUsage,PCRFIPAddress,AccountNextResetTime,UEIPv6,ANGWAddress,AccessNodeStatus,TAI,ECGI,UnderControl,OldQuotaStatus,OldQuotaConsumption,OldQuotaBalance,QuotaResetTime,OldAccountStatus,OldAccountPrivilege,OldAccountBalance,OldAccountConsumption,AccountResetTime,QuotaLastResetTime,RoamingRegion2,RoamingRegion3,RoamingRegion4,RoamingRegion5,RoamingRegion6,RoamingRegion7,RoamingRegion8,RoamingRegion9,RoamingRegion10,QuotaValueOrStatus,VisitServiceZone2,VisitServiceZone3,VisitServiceZone4,VisitServiceZone5,AccumulationValue,AccumulationBalance,AccumulationConsumption,AccumulationStartTime,AccumulationEndTime,AccumulationExpireFlag,OldAccumulationValue,OldAccumulationBalance,OldAccumulationConsumption,OldAccumulationStartTime,OldAccumulationEndTime,ActivationTimeValue,ActivationStartTime,ActivationEndTime,RestTimeValue,PLMNIdentify,TimeZone,QuotaValue,ServiceStartDateTime,ServiceEndDateTime,ServiceDescription,ServiceCustomerAttribute,VisitServiceZone6,VisitServiceZone7,VisitServiceZone8,VisitServiceZone9,VisitServiceZone10,SubscribedDateTime,RARCause,TransinSubscriberID,TransinQuotaName,TransferQuotaValue,TransferQuotaStartTime,TransferQuotaEndTime,NotificationIndication,TransferQuotaBalance,TransferQuotaConsumption,TransferTimeStamp,OldActivationStartTime,OldActivationEndTime,OldRestTimeValue,Trans_inServiceName,Trans_inServiceCustomerAttribute,BillingCycleDate,SessionId,OriginHost,OriginRealm,DestinationHost,DestinationRealm,RxIPV4,RxIPV6,LastAccumulateValue,CurrentCumulateTimes,RxMediaType,RxSpecificAction,SyPolicyCounterIdentifier,SyPolicyCounterStatus,SyPendingPolicyCounterstatus1,SyPendingPolicyCounterChangeTime1,SyPendingPolicyCounterstatus2,SyPendingPolicyCounterChangeTime2,SyPendingPolicyCounterstatus3,SyPendingPolicyCounterChangeTime3,SyResultCode,QCI,ClientlessOptimizationRule,ContentFilteringRule,FUP,HTTPEnrichmentRule,BMIIMEI,BMIIMSI,OperatorID,BMIRoamingStatus,ServiceExtendedAttribute3,QuotaExtendedAttribute,Transfer,VisitServiceZone11,VisitServiceZone12,VisitServiceZone13,VisitServiceZone14,VisitServiceZone15,VisitServiceZone16,VisitServiceZone17,VisitServiceZone18,VisitServiceZone19,VisitServiceZone20,ActualQuotaUsage,RateDiscount,ActualAccountUsage,usrStatus,ServiceUsageState,Reserved,Reserved1,Reserved2,ServiceExtendedAttribute1,ServiceExtendedAttribute2,NetLocAccessSupport,TransactionId,QuotaSequenceId,OldQuotaValue,QuotaExpiredTime,OldQuotaExpiredTime,OldQuotaSequenceId,IfrsParam",
                 name = "upcc")

    # laccima 3G
    createScheme(path=path_ref_lacima_3g,
                 delimiter="|",
                 schemaString="lac|cell_id|site_name|vendor|site_id|ne_id|bsc_name|msc_name|node|longitude|latitude|azimuth|regional_network|propinsi|kabupaten|kecamatan|kelurahan|regional_channel|branch|subbranch|address|sector|cluster|mitra_ad|cgi_prepaid|cgi_postpaid|ocs_cluster|ocs_zone|type_lacci|on_air_date|eci",
                 name = "lacima_3g")

    # laccima 4G
    createScheme(path=path_ref_lacima_4g,
                 delimiter="|",
                 schemaString="lac|cell_id|site_name|vendor|site_id|ne_id|bsc_name|msc_name|node|longitude|latitude|azimuth|regional_network|propinsi|kabupaten|kecamatan|kelurahan|regional_channel|branch|subbranch|address|sector|cluster|mitra_ad|cgi_prepaid|cgi_postpaid|ocs_cluster|ocs_zone|type_lacci|on_air_date|eci",
                 name = "lacima_4g")

    ########## 3. Apply Query to agg table ##########
    # Create laccima
    applyQuery(path_query_lacima)

    # Create output
    applyQuery(path_query_output)

    ########## 4. Output ##########
    sqlContext.sql("select * from output").map(toCSVLine).coalesce(6).saveAsTextFile(path_output)

