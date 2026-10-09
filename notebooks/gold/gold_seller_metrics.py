#!/usr/bin/env python
# coding: utf-8

# ## gold_seller_metrics
# 
# null

# In[1]:


# Gold Seller Metrics

from pyspark.sql.functions import (
    countDistinct,
    count,
    sum,
    avg,
    min,
    max,
    round,
    col
)

# Read source tables
df_sales = spark.table("gold_fact_sales")
df_reviews = spark.table("silver_order_reviews")

# Join reviews
df_seller_sales = (
    df_sales
    .join(
        df_reviews.select(
            "order_id",
            "review_score"
        ),
        on="order_id",
        how="left"
    )
)

# Aggregate seller metrics
df_seller_metrics = (
    df_seller_sales
    .groupBy(
        "seller_id",
        "seller_city",
        "seller_state"
    )
    .agg(
        countDistinct("order_id")
            .alias("orders_count"),

        count("product_id")
            .alias("products_sold"),

        round(
            sum("price") + sum("freight_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("price"),
            2
        ).alias("avg_product_price"),

        round(
            avg("review_score"),
            2
        ).alias("avg_review_score"),

        min("purchase_date")
            .alias("first_sale_date"),

        max("purchase_date")
            .alias("last_sale_date")
    )
)

# Display result
display(df_seller_metrics)

# Save Gold table
df_seller_metrics.write \
    .mode("overwrite") \
    .saveAsTable("gold_seller_metrics")

