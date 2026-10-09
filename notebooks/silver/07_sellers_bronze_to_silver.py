#!/usr/bin/env python
# coding: utf-8

# ## 07_sellers_bronze_to_silver
# 
# null

# In[1]:


# Transform sellers data to silver layer
# Przygotowuję dane orders do warstwy silver

from pyspark.sql.functions import col

df_sellers = spark.read.csv("Files/Bronze/sellers/olist_sellers_dataset.csv"
,header=True
,inferSchema=True)

display(df_sellers)


# In[9]:


# df_sellers.filter(col("seller_id").isNull()).count()
# display(df_sellers.groupBy("seller_id").count().filter(col("count")>1))
df_sellers = df_sellers.filter(col("seller_id").isNotNull()).dropDuplicates(["seller_id"])


# In[10]:


df_sellers.write.mode("overwrite").saveAsTable("silver_sellers")

