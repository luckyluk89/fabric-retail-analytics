#!/usr/bin/env python
# coding: utf-8

# ## 01_orders_bronze_to_silver
# 
# null

# In[3]:


# Transform orders data to silver layer
# Przygotowuję dane orders do warstwy silver


# In[4]:


from pyspark.sql.functions import datediff,col

df_orders = spark.read.csv("Files/Bronze/orders/olist_orders_dataset.csv"
,header=True
,inferSchema=True)


# In[5]:


# create ready to silver orders data frame
df_orders_silver = df_orders.filter(col("order_id").isNotNull()).dropDuplicates(["order_id"])
df_orders_silver = df_orders_silver.withColumn("delivery_days" \
,datediff("order_delivered_customer_date","order_purchase_timestamp"))


# In[7]:


# create a delta table with orders silver data
df_orders_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")


# In[8]:


display(spark.table("silver_orders"))

