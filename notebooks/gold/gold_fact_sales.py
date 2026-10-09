#!/usr/bin/env python
# coding: utf-8

# ## gold_fact_sales
# 
# null

# In[1]:


# Loading needed delta tables to data frames

df_order_items = spark.table("silver_order_items")
df_orders = spark.table("silver_orders")
df_customers = spark.table("silver_customers")
df_products = spark.table("silver_products")
df_product_translation = spark.table("silver_product_translation")
df_sellers = spark.table("silver_sellers")


# In[10]:


df_sales = df_order_items.join(
   df_orders,
   on="order_id",
   how="left" 
)

df_sales = df_sales.join(
df_customers,
on="customer_id",
how="left"

)


# In[11]:


df_sales = df_sales.join(
        df_products,
        on="product_id",
        how="left"
    )

df_sales = (
    df_sales
    .join(
        df_product_translation,
        on="product_category_name",
        how="left"
    )
)

df_sales = (
    df_sales
    .join(
        df_sellers,
        on="seller_id",
        how="left"
    )
)


# In[4]:


df_sales.printSchema()


# In[6]:


df_sales.printSchema()


# In[12]:


from pyspark.sql.functions import col, to_date, datediff

df_sales = (
    df_sales
    .withColumn(
        "purchase_date",
        to_date(col("order_purchase_timestamp"))
    )
    .withColumn(
        "approved_date",
        to_date(col("order_approved_at"))
    )
    .withColumn(
        "delivered_date",
        to_date(col("order_delivered_customer_date"))
    )
    .withColumn(
        "estimated_delivery_date",
        to_date(col("order_estimated_delivery_date"))
    )
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
)


# In[8]:


df_sales.printSchema()


# In[13]:


df_sales = df_sales.select(
    # Klucze
    "order_id",
    "customer_id",
    "product_id",
    "seller_id",

    # Daty i timestampy
    "order_purchase_timestamp",
    "purchase_date",

    "order_approved_at",
    "approved_date",

    "order_delivered_customer_date",
    "delivered_date",

    "order_estimated_delivery_date",
    "estimated_delivery_date",

    "delivery_days",

    # Zamówienie
    "order_status",

    # Finanse
    "price",
    "freight_value",

    # Produkt
    "product_category_name_english",
    "product_weight_grams",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",

    # Klient
    "customer_city",
    "customer_state",

    # Sprzedawca
    "seller_city",
    "seller_state"
)


# In[14]:


# df_sales.printSchema()


# In[15]:


# display(df_sales)


# In[16]:


df_sales.write.mode("overwrite").saveAsTable("gold_fact_sales")

