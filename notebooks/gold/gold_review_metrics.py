#!/usr/bin/env python
# coding: utf-8

# ## gold_review_metrics
# 
# null

# In[1]:


# Gold Review Metrics

from pyspark.sql.functions import (
    round,
    avg,
    countDistinct,
    count
)

# Read source tables
df_sales = spark.table("gold_fact_sales")
df_reviews = spark.table("silver_order_reviews")

# Join fact sales with reviews
df_review_metrics = (
    df_sales
    .join(
        df_reviews.select(
            "order_id",
            "review_score"
        ),
        on="order_id",
        how="inner"
    )
)

# Aggregate by delivery time
df_review_metrics = (
    df_review_metrics
    .groupBy("delivery_days")
    .agg(
        countDistinct("order_id")
            .alias("orders_count"),

        count("review_score")
            .alias("reviews_count"),

        round(
            avg("review_score"),
            2
        ).alias("avg_review_score")
    )
)

# Display
display(df_review_metrics)

# Save Gold table
df_review_metrics.write \
    .mode("overwrite") \
    .saveAsTable("gold_review_metrics")

