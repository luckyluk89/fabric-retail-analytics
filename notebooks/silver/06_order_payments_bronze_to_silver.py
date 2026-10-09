#!/usr/bin/env python
# coding: utf-8

# ## 06_order_payments_bronze_to_silver
# 
# null

# In[ ]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[10]:


from pyspark.sql.functions import col

path = "Files/Bronze/order_payments"

df_order_payments = spark.read.csv(
    "Files/Bronze/order_payments"
    ,header=True
    ,inferSchema=True
)
# display(df_order_payments)


# In[11]:


df_order_payments = df_order_payments.filter(col("order_id").isNotNull()).dropDuplicates(["order_id"])


# In[12]:


df_order_payments.write.mode("overwrite").saveAsTable("silver_order_payments")

