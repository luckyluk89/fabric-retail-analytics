#!/usr/bin/env python
# coding: utf-8

# ## 05_geolocation_bronze_to_silver
# 
# null

# In[5]:


# Transform geolocation data to silver layer
# Przygotowuję dane orders do warstwy silver


# In[6]:


from pyspark.sql.functions import col

df_geolocation = spark.read.csv("Files/Bronze/geolocation/olist_geolocation_dataset.csv",
header=True
,inferSchema=True)

display(df_geolocation)


# In[11]:


# create ready to silver orders data frame

df_geolocation.filter(col("geolocation_zip_code_prefix").isNull()).count()
df_geolocation.filter(col("geolocation_zip_code_prefix").isNotNull()).dropDuplicates(["geolocation_zip_code_prefix"])


# In[13]:


# create a delta table with orders silver data

df_geolocation.write.mode("overwrite").saveAsTable("silver_geolocation")

