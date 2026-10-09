#!/usr/bin/env python
# coding: utf-8

# ## 02_order_items_bronze_to_silver
# 
# null

# In[13]:


# Transform order_items data to silver layer
# Przygotowuję dane orders do warstwy silver

from pyspark.sql.functions import col

bronze_path = "Files/Bronze/order_items/olist_order_items_dataset.csv"

df_order_items = spark.read.csv(bronze_path
,header=True
,inferSchema=True
)


# In[2]:


# display(df_order_items)


# In[9]:


# Basic quality checks
# df_order_items.filter(col("order_id").isNull()).count()
# print(df_order_items.count())
# print(df_order_items.dropDuplicates().count())
# df_order_items.printSchema()


# In[14]:


# create ready to silver order_items data frame and write date into delta table
df_order_items_silver = df_order_items.filter(col("order_id").isNotNull())
df_order_items_silver.write.mode("overwrite").saveAsTable("silver_order_items")


# In[15]:


display(spark.table("silver_order_items"))

