#!/usr/bin/env python
# coding: utf-8

# ## gold_customer_metrics
# 
# null

# In[1]:


# Gold Customer Metrics

from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum,
    avg,
    min,
    max,
    round,
    datediff
)

# Read gold fact table
df_sales = spark.table("gold_fact_sales")

# Calculate average order value per customer
df_avg_order_value = (
    df_sales
    .groupBy(
        "customer_id",
        "order_id"
    )
    .agg(
        (
            sum("price") +
            sum("freight_value")
        ).alias("order_value")
    )
    .groupBy("customer_id")
    .agg(
        round(
            avg("order_value"),
            2
        ).alias("avg_order_value")
    )
)

# Customer metrics
df_customer_metrics = (
    df_sales
    .groupBy(
        "customer_id",
        "customer_city",
        "customer_state"
    )
    .agg(
        countDistinct("order_id").alias("orders_count"),

        count("product_id").alias("products_purchased"),

        round(
            sum("price") + sum("freight_value"),
            2
        ).alias("total_spent"),

        round(
            avg("price"),
            2
        ).alias("avg_product_price"),

        min("purchase_date").alias("first_purchase_date"),

        max("purchase_date").alias("last_purchase_date")
    )
    .withColumn(
        "customer_lifetime_days",
        datediff(
            col("last_purchase_date"),
            col("first_purchase_date")
        )
    )
    .join(
        df_avg_order_value,
        on="customer_id",
        how="left"
    )
)

# Display result
display(df_customer_metrics)

# Save table
df_customer_metrics.write \
    .mode("overwrite") \
    .saveAsTable("gold_customer_metrics")

