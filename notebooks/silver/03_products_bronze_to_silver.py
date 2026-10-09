#!/usr/bin/env python
# coding: utf-8

# ## 03_products_bronze_to_silver
# 
# null

# In[6]:


# Transform products data to silver layer
# Przygotowuję dane orders do warstwy silver

from pyspark.sql.functions import col
path = "Files/Bronze/products/olist_products_dataset.csv"

df_products = spark.read.csv(
    path
    ,header=True
    ,inferSchema=True
)

display(df_products)


# In[7]:


# Basic quality checks

# df_products.filter(col("product_id").isNull()).count()
# df_products.count()
# df_products.dropDuplicates().count()
# df_products.printSchema()


# In[8]:


df_products_silver = df_products.filter(col("product_id").isNotNull())
df_products_silver = df_products_silver.withColumnRenamed("product_weight_g","product_weight_grams")
df_products_silver.write.mode("overwrite").saveAsTable("silver_products")

display(spark.table("silver_products"))


# In[ ]:




