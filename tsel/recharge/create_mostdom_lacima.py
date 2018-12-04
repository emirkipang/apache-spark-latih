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

    ########## 1. Init path ##########
    # ref
    path_ref_lacima_3g = "file:///data01/hnat_qsr/spark/dev/ref/v_par_ref_lacima.csv"
    path_ref_lacima_4g = "file:///data01/hnat_qsr/spark/dev/ref/v_par_ref_lacima_4g.csv"
    path_ref_mostdom = "file:///data01/hnat_qsr/spark/dev/ref/run_mostdom.csv"
    # query
    path_query_lacima = "/data01/hnat_qsr/spark/dev/query/lacima.sql"
    path_query_mostdom_lacima = "/data01/hnat_qsr/spark/dev/query/mostdom_lacima.sql"
    # output
    path_output = "file:///data01/hnat_qsr/spark/dev/ref/mostdom_lacima.csv"

    ########## 2. Load data to source table ##########
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
    createScheme(path=path_ref_mostdom,
                 delimiter="|",
                 schemaString="tksparty mnc_mcc lac ci cnt_record sum_charge rule_period",
                 name="mostdom")

    ########## 3. Apply Query to agg table ##########
    # Create laccima
    applyQuery(path_query_lacima)

    # create mostdom
    applyQuery(path_query_mostdom_lacima)

    ########## 4. Output ##########
    sqlContext.sql("select * from mostdom_lacima").map(toCSVLine).coalesce(10).saveAsTextFile(path_output)

