# Databricks notebook source
# DBTITLE 1,import library
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,data frame
df = spark.read.load('/FileStore/tables/googleplaystore-1.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,data cleaning
df = df.drop("size","content Rating","Last Updated","Android Ver", "Current Ver")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df=df.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
    .withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
        .withColumn("Installs",col("Installs").cast(IntegerType()))\
            .withColumn("Price",regexp_replace(col("price"),"[$]",""))\
                .withColumn("Price",col("Price").cast(IntegerType()))

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView('apps')

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top reviews give to the apps
# MAGIC %sql select App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc 

# COMMAND ----------

# DBTITLE 1,top 10 installs app
# MAGIC %sql select App,Type, sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,category wise distrubution
# MAGIC %sql select Category,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,top paid apps
# MAGIC %sql select App,sum(price) from apps
# MAGIC where Type = 'Paid'
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------


