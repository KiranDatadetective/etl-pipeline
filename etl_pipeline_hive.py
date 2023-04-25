from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import re
from delta.pip_utils import configure_spark_with_delta_pip

import findspark
findspark.init()

builder = SparkSession.builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.shuffle.partitions",1)\
            .config("spark.default.parallelism",1)\
            .config("spark.rdd.compress",False)\
            .config("spark.shuffle.compress",False)\
            .config("spark.databricks.delta.allowArbitraryProperties.enabled",True)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "/D:/Programs/etl-powerbi/dataset/covid_worldwide.csv"

df = spark.read.csv(path, header=True)

# new_text = re.sub(r"[^0-9]","",text)

df_clean = df.withColumn("Total Cases", (regexp_replace(col("Total Cases"),"[^0-9]", "")).cast(IntegerType()))\
             .withColumn("Total Deaths", (regexp_replace(col("Total Deaths"),"[^0-9]", "")).cast(IntegerType()))\
             .withColumn("Total Recovered", (regexp_replace(col("Total Recovered"),"[^0-9]", "")).cast(IntegerType()))\
             .withColumn("Active Cases", (regexp_replace(col("Active Cases"),"[^0-9]", "")).cast(IntegerType()))\
             .withColumn("Total Test", (regexp_replace(col("Total Test"),"[^0-9]", "")).cast(IntegerType()))\
             .withColumn("Population", (regexp_replace(col("Population"),"[^0-9]", "")).cast(IntegerType()))


out_df = df_clean.withColumn("Recovered_percent", round((col("Total Recovered")/col("Total Cases"))*100,2))\
           .withColumn("Deaths_percent", round((col("Total Deaths")/col("Total Cases"))*100,2))\
           .withColumn("Active_cases_percent", round((col("Active Cases")/col("Total Cases"))*100,2))\
           .withColumn("Total_cases_percent", round((col("Total Cases")/col("Total Test"))*100,2))\
           .withColumn("Total_test_percent", round((col("Total Test")/col("Population"))*100,2))


print("df_clean\n",df_clean.show())

# spark.catalog.setCurrentDatabase("covid")
# df_clean.createOrReplaceTempView("clean_view")
# spark.sql('''merge into case_data
#              using clean_view
#                    on case_data.`Serial Number` = clean_view.`Serial Number`
#              when matched then
#              update set
#                         case_data.`Serial Number` = clean_view.`Serial Number`,
#                         case_data.Country = clean_view.Country,
#                         case_data.`Total Cases` = clean_view.`Total Cases`,
#                         case_data.`Total Deaths` = clean_view.`Total Deaths`,
#                         case_data.`Total Recovered` = clean_view.`Total Recovered`,
#                         case_data.`Active Cases` = clean_view.`Active Cases`,
#                         case_data.`Total Test` = clean_view.`Total Test`,
#                         case_data.Population = clean_view.Population
#              when not matched then insert *''')
#
# # print(out_df.show())
# df_read = spark.sql("select * from case_data")
# print("df_read\n",df_read.show())
cases_data_path = "D:/Programs/etl-powerbi/tables/case_data"
metrix_path = "D:/Programs/etl-powerbi/tables/metrix"

# df_clean.write.format("deta").mode("overwrite").option("overwriteSchema","true").option("path",cases_data_path).saveAsTable("case_data")
# out_df.write.format("deta").mode("overwrite").option("overwriteSchema","true").option("path",metrix_path).saveAsTable("metrix")