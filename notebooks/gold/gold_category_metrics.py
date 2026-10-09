#!/usr/bin/env python
# coding: utf-8

# ## gold_category_metrics
# 
# null

# In[1]:


# Gold Category Metrics

from pyspark.sql.functions import (
    countDistinct,
    count,
    sum,
    avg,
    round
)

# Read source tables
df_sales = spark.table("gold_fact_sales")
df_reviews = spark.table("silver_order_reviews")

# Join review scores
df_category_sales = (
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

# Aggregate metrics
df_category_metrics = (
    df_category_sales
    .groupBy("product_category_name_english")
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
        ).alias("avg_review_score")
    )
)

# Display
display(df_category_metrics)

# Save table
df_category_metrics.write \
    .mode("overwrite") \
    .saveAsTable("gold_category_metrics")

