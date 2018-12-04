from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BENTANG = "salary_midpoint_bentang"

if __name__ == "__main__":
    session = SparkSession.builder.master("local[2]").getOrCreate()
        #.appName("Survey stack overflow")\

    dataFrameReader = session.read

    responses = dataFrameReader\
        .option("header","true")\
        .option("inferSchema", value=True)\
        .csv("../in/2016-stack-overflow-survey-responses.csv")

    print("========== Tampilkan Schema ==========")
    # responses.printSchema()

    responColumn = responses.select("country","occupation",SALARY_MIDPOINT,AGE_MIDPOINT)
    # responColumn.show()

    print("========== Filter country ==========")
    responColumn.filter(
        (responColumn["country"]=="Afghanistan") | # OR
        (responColumn["country"] == "Albania")
    )\
        # .show()

    print("========== Group by occupation ==========")
    responColumn.groupBy("occupation").count()\
        # .show()

    print("========== Responder umur dibawah 20 ==========")
    responColumn.filter(
        (responColumn[AGE_MIDPOINT]<20)
    )\
        # .show()

    print("========== Data salary midpoint diurut dari yg terbesar ==========")
    # responColumn.orderBy(responColumn[SALARY_MIDPOINT], ascending = False).show()

    print("========== Salary midpoint dengan bentangan ==========")
    salaryBentangan = responses.withColumn(SALARY_MIDPOINT_BENTANG,((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))
    # salaryBentangan.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BENTANG).show()

    print("========== Jumlah developer berdasarkan bentangan terendah ==========")
    # salaryBentangan.groupBy(SALARY_MIDPOINT_BENTANG).count().orderBy(SALARY_MIDPOINT_BENTANG, ascending = False).show()


    # 1. group by location


    # 2. group by location + avg column harga tanah