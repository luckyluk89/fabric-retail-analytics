#!/usr/bin/env python
# coding: utf-8

# ## 04_customers_bronze_to_silver
# 
# 

# In[2]:


# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql.functions import col
path = "Files/Bronze/customers/olist_customers_dataset.csv"
df_customers = spark.read.csv(
    path
    ,header=True
    ,inferSchema=True
)

display(df_customers)


# In[7]:


#basic quality checks

# df_customers.filter(col("customer_unique_id").isNull()).count()
# df_customers.count()
# df_customers.dropDuplicates().count()
# df_customers.printSchema()


# In[9]:


#create silver data frame

df_customers_silver = df_customers.filter(col("customer_unique_id").isNotNull())
df_customers_silver.write.mode("overwrite").saveAsTable("silver_customers")


# In[10]:


display(spark.table("silver_customers"))

