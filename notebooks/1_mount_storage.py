# Databricks notebook source
# MAGIC %md
# MAGIC # Mount blob storage
# MAGIC 
# MAGIC Mounting blob storage containers in Azure Databricks allows you to access blob storage containers like they are directories.<BR>
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define credentials
# MAGIC To mount blob storage - we need storage credentials - storage account name and storage account key

# COMMAND ----------

# Replace with your storage account name
storageAccountName = "<storageAccountName>"
storageAccountAccessKey = '<storageAccountAccessKey>'

# COMMAND ----------

# MAGIC %md
# MAGIC Create the base mount directory

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/ong-new-mexico

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Mount blob storage

# COMMAND ----------

# This is a function to mount a storage container
def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, blobMountPoint):
  try:
    print("Mounting {0} to {1}:".format(storageContainer, blobMountPoint))
    # Unmount the storage container if already mounted
    dbutils.fs.unmount(blobMountPoint)
  except Exception as e:
    # If this errors, safe to assume that the container is not mounted
    print("....Container is not mounted; Attempting mounting now..")
    
  # Mount the storage container
  mountStatus = dbutils.fs.mount(
                  source = "wasbs://{0}@{1}.blob.core.windows.net/".format(storageContainer, storageAccount),
                  mount_point = blobMountPoint,
                  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storageAccount): storageAccountKey})

  print("....Status of mount is: " + str(mountStatus))
  print() # Provide a blank line between mounts

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount the various storage containers created

# COMMAND ----------

#wsproduction-raw-zip raw zipped file from ftp site
mountStorageContainer(storageAccountName,storageAccountAccessKey,"nm-wsproduction-raw-zip","/mnt/ong-new-mexico/nm-wsproduction-raw-zip")

# COMMAND ----------

#wsproduction-raw raw unzipped file from ftp site
mountStorageContainer(storageAccountName,storageAccountAccessKey,"nm-wsproduction-raw","/mnt/ong-new-mexico/nm-wsproduction-raw")

# COMMAND ----------

#nm-wsproduction-streaming to hold streaming files
mountStorageContainer(storageAccountName,storageAccountAccessKey,"nm-wsproduction-streaming","/mnt/ong-new-mexico/nm-wsproduction-streaming")

# COMMAND ----------

#nm-wsproduction-processed-csv to hold the csv fies
mountStorageContainer(storageAccountName,storageAccountAccessKey,"nm-wsproduction-processed-csv","/mnt/ong-new-mexico/nm-wsproduction-processed-csv")

# COMMAND ----------

#nm-wsproduction-staging Staging Directory after spark sql parsing
mountStorageContainer(storageAccountName,storageAccountAccessKey,"nm-wsproduction-staging","/mnt/ong-new-mexico/nm-wsproduction-staging")


# COMMAND ----------

#Display conatiners
display(dbutils.fs.ls("/mnt/ong-new-mexico"))

# COMMAND ----------

