# Databricks notebook source
# MAGIC %md
# MAGIC #Import Libraries

# COMMAND ----------

# 0)  Import Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# COMMAND ----------

# MAGIC %md
# MAGIC #Declare Storage Variables

# COMMAND ----------

# 1) Source directory
dbfsSrcDirPath="/mnt/ong-new-mexico/nm-wsproduction-raw"

# 2) Source File
sourceFile = dbfsSrcDirPath + "/wcproduction.xml"

# 3) Streaming directory
dbfsSrcDirPathStreaming="/mnt/ong-new-mexico/nm-wsproduction-streaming"

# 4) Destination directory
dbfsDestDirPath="/mnt/ong-new-mexico/nm-wsproduction-processed-csv"

# 5) Staging Directory after spark sql parsing
dbfsStagingDir="/mnt/ong-new-mexico/nm-wsproduction-staging"


# COMMAND ----------

# MAGIC %fs head --maxBytes=5000 /mnt/ong-new-mexico/nm-wsproduction-raw/wcproduction.xml

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Spark Streaming Context

# COMMAND ----------

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
ssc = StreamingContext(sc,1)

# COMMAND ----------

dfStreamingText = ssc.textFileStream(dbfsSrcDirPathStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC #Start streaming the large xml file with lineSep = '>'

# COMMAND ----------

text_sdf = spark.readStream.text("/mnt/ong-new-mexico/nm-wsproduction-raw/wcproduction*.xml",lineSep='>' )
text_sdf.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC #Write the streaming query results in 20 second increments in small files

# COMMAND ----------

write_stream = text_sdf.writeStream.trigger(processingTime='20 seconds').start(path='/mnt/ong-new-mexico/nm-wsproduction-streaming/wcproduction_smallchunks', queryName='wcprod_query', outputMode="append", format='text',checkpointLocation='/mnt/ong-new-mexico/nm-wsproduction-streaming/checkpoint')


# COMMAND ----------

# MAGIC %md
# MAGIC #Cancel the streaming job once it is done processing the files

# COMMAND ----------

# MAGIC %md
# MAGIC #Create a new DB and table to further process the small files from streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ong_new_mexico;
# MAGIC 
# MAGIC USE ong_new_mexico;
# MAGIC --DROP TABLE wcproduction;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS wcproduction(
# MAGIC   Value STRING )
# MAGIC   LOCATION "dbfs:/mnt/ong-new-mexico/nm-wsproduction-streaming/wcproduction_smallchunks"
# MAGIC   STORED AS TEXTFILE;

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean Up Step1
# MAGIC # 1. Remove special character
# MAGIC # 2. Remove value after and including xmlns
# MAGIC # 3. Add closing >

# COMMAND ----------

sourceDF = spark.sql("SELECT concat(split(regexp_replace(decode(value,'UTF-16'),'�',''),' xmlns')[0],'>') AS value from ong_new_mexico.wcproduction")

# COMMAND ----------

sourceDF.head(100)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean up Step 2 to facilate xml parsing
# MAGIC # 1. Remove xsd lines
# MAGIC # 2. Remove root line

# COMMAND ----------

modDF = sourceDF.filter(~sourceDF.value.contains('xsd')).filter(~sourceDF.value.contains('root'))

# COMMAND ----------

modDF.head(100)

# COMMAND ----------

modDF.write.text(dbfsStagingDir+"/nm-wsproduction-staging")

# COMMAND ----------

# MAGIC %md
# MAGIC # Quick test to see if there are more characters to clean
# MAGIC # We found a \n

# COMMAND ----------

testFile = '/mnt/ong-new-mexico/nm-wsproduction-staging/nm-wsproduction-staging/part-00000-tid-4163187932729680924-b422d691-4382-4048-8784-27bd61278598-337-1-c000.txt'

# COMMAND ----------

dbutils.fs.head(testFile)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quick test
# MAGIC # Remember to install the spark xml library
# MAGIC # Coordinate
# MAGIC # com.databricks:spark-xml:0.5.0

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
sourceDFXMLFinalTest = spark.read.format('xml').options(rowTag='wcproduction').options(charset='UTF-8').load(testFile)

# COMMAND ----------

display(sourceDFXMLFinalTest)

# COMMAND ----------

# MAGIC %md
# MAGIC # Denote source and destination

# COMMAND ----------

sourceXMLFiles =dbfsStagingDir+"/nm-wsproduction-staging/part*.txt"

# COMMAND ----------

destFolder = dbfsDestDirPath + '/wcproduction'

# COMMAND ----------

# MAGIC %md
# MAGIC # Start the xml read to parse the values
# MAGIC # Remember to install the spark xml library
# MAGIC # Coordinate
# MAGIC # com.databricks:spark-xml:0.5.0

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
sourceDFXMLFinal = spark.read.format('xml').options(rowTag='wcproduction').options(charset='UTF-8').load(sourceXMLFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Clean up of the values obtained after xml parsing

# COMMAND ----------

cleanedSourceDFXMLFinal = sourceDFXMLFinal.withColumn('amend_ind', regexp_replace('amend_ind', '\n', ''))\
.withColumn('api_cnty_cde', regexp_replace('api_cnty_cde', '\n', ''))\
.withColumn('api_st_cde', regexp_replace('api_st_cde', '\n', ''))\
.withColumn('api_well_idn', regexp_replace('api_well_idn', '\n', ''))\
.withColumn('c115_wc_stat_cde', regexp_replace('c115_wc_stat_cde', '\n', ''))\
.withColumn('eff_dte', regexp_replace('eff_dte', '\n', ''))\
.withColumn('mod_dte', regexp_replace('mod_dte', '\n', ''))\
.withColumn('ogrid_cde', regexp_replace('ogrid_cde', '\n', ''))\
.withColumn('pool_idn', regexp_replace('pool_idn', '\n', ''))\
.withColumn('prd_knd_cde', regexp_replace('prd_knd_cde', '\n', ''))\
.withColumn('prod_amt', regexp_replace('prod_amt', '\n', ''))\
.withColumn('prodn_day_num', regexp_replace('prodn_day_num', '\n', ''))\
.withColumn('prodn_mth', regexp_replace('prodn_mth', '\n', ''))\
.withColumn('prodn_yr', regexp_replace('prodn_yr', '\n', ''))

# COMMAND ----------

display(cleanedSourceDFXMLFinal)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to the destination folder as csv

# COMMAND ----------

cleanedSourceDFXMLFinal.write.format("com.databricks.spark.csv").option("header", "true").save(destFolder)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quick tests

# COMMAND ----------

cleanedSourceDFXMLFinal.count()

# COMMAND ----------

