# Databricks notebook source
# MAGIC %md
# MAGIC #Declare Storage Variables

# COMMAND ----------

# 1) Raw zipped directory
dbfsSrcDirPathRawZipped="/mnt/ong-new-mexico/nm-wsproduction-raw-zip"

# 2) Raw unzipped directory
dbfsSrcDirPathRaw="/mnt/ong-new-mexico/nm-wsproduction-raw"

# 3) Check first few lines
dbfsSrcRawFile = dbfsSrcDirPathRaw + "/wcproduction.xml"


# COMMAND ----------

# MAGIC %md
# MAGIC #Retrieve the zipped file from the ftp site

# COMMAND ----------

#Retrieve the zipped file
import urllib 
urllib.request.urlretrieve("ftp://164.64.106.6/Public/OCD/OCD%20Interface%20v1.1/volumes/wcproduction/wcproduction.zip", "/tmp/wcproduction.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC #Unzip the file from the /tmp location

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /tmp/wcproduction.zip

# COMMAND ----------

# MAGIC %md
# MAGIC #Move the xml file to the blob storage

# COMMAND ----------

#Move temp file to DBFS
dbutils.fs.mv("file:/databricks/driver/wcproduction.xml", dbfsSrcRawFile)  

# COMMAND ----------

#Check first few lines
dbutils.fs.head(dbfsSrcRawFile)

# COMMAND ----------

