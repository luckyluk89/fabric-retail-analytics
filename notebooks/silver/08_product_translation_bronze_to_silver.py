#!/usr/bin/env python
# coding: utf-8

# ## 08_product_translation_bronze_to_silver
# 
# null

# In[1]:


# Transform product translation data to silver layer
# Przygotowuję dane orders do warstwy silver

from pyspark.sql.functions import col

df_product_translation = spark.read.csv("Files/Bronze/product_category_translation"
,header=True
,inferSchema=True)

display(df_product_translation)


# In[3]:


# df_product_translation.filter(col("product_category_name").isNull()).count()
# display(df_product_translation.groupBy("product_category_name").count().filter(col("count")>1))
df_product_translation = df_product_translation.filter(col("product_category_name").isNotNull()).dropDuplicates(["product_category_name"])
display(df_product_translation)


# In[4]:


df_product_translation.write.mode("overwrite").saveAsTable("silver_product_translation")

