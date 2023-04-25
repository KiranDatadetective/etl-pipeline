from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import psycopg2

import findspark
findspark.init()

builder = SparkSession.builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars", "C:\spark-3.3.2-bin-hadoop3\jars\postgresql-42.6.0.jar")\
            .config("spark.sql.shuffle.partitions",1)\
            .config("spark.default.parallelism",1)\
            .config("spark.rdd.compress",False)\
            .config("spark.shuffle.compress",False)\
            .config("spark.databricks.delta.allowArbitraryProperties.enabled",True)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "/D:/Programs/etl-powerbi/dataset/covid_worldwide.csv"

df = spark.read.csv(path, header=True)

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

out_df = out_df.selectExpr('`Serial Number` as Serial_number','Country',
                              'Recovered_percent','Deaths_percent','Active_cases_percent','Total_cases_percent','Total_test_percent')


mode = "overwrite"
url = "jdbc:postgresql://localhost:5432/etl"
properties = {"user": "postgres","password": "1234","driver": "org.postgresql.Driver"}
out_df.write.jdbc(url=url, table="temp_view", mode=mode, properties=properties)

conn = psycopg2.connect(database="etl",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")


cursor = conn.cursor()
cursor.execute("""MERGE INTO patient_final as p USING temp_view as t
                  ON p."Serial_number" = t."Serial_number"
                  WHEN MATCHED THEN
                    UPDATE SET "Serial_number" = t."Serial_number",
                            "Country" = t."Country",
                           "Recovered_percent" = t."Recovered_percent",
                           "Deaths_percent" = t."Deaths_percent",
                           "Active_cases_percent" = t."Active_cases_percent",
                           "Total_cases_percent" = t."Total_cases_percent",
                           "Total_test_percent" = t."Total_test_percent"
                  WHEN NOT MATCHED THEN
                        INSERT ("Serial_number","Country", "Recovered_percent","Deaths_percent","Active_cases_percent","Total_cases_percent","Total_test_percent")
                        VALUES (t."Serial_number",t."Country", t."Recovered_percent",t."Deaths_percent",t."Active_cases_percent",t."Total_cases_percent",t."Total_test_percent")""")
conn.commit()
# print(cursor.fetchall())

# jdbcDF_temp = spark.read.jdbc(url=url, table="temp_view", properties=properties)
jdbcDF = spark.read.jdbc(url=url, table="patient_final", properties=properties)
print(out_df.show())
print(jdbcDF.show())