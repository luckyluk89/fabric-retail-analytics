#!/usr/bin/env python
# coding: utf-8

# ## 09_order_reviews_bronze_to_silver
# 
# null

# In[16]:


# Transform order_reviews data to silver layer
# Przygotowuję dane orders do warstwy silver

from pyspark.sql.functions import  row_number, col
path = "Files/Bronze/order_reviews/olist_order_reviews_dataset.csv"
df_order_reviews = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")  # <- TA LINIA JEST KLUCZOWA
    .load(path)
)

display(df_order_reviews)


# In[22]:


# df_order_reviews.printSchema()
# df_order_reviews.filter(col("review_id").isNull()).count()
df_order_reviews = df_order_reviews.filter(col("order_id").isNotNull())

duplicate_orders = (
    df_order_reviews
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .select("order_id")
)

display(
    df_order_reviews
    .join(
        duplicate_orders,
        on="order_id",
        how="inner"
    )
    .select(
        "order_id",
        "review_creation_date",
        "review_answer_timestamp",
        "review_score",
        "review_comment_title",
        "review_comment_message"
    )
    .orderBy(
        col("order_id"),
        col("review_creation_date").desc()
    )
)

# df_order_reviews = df_product_translation.filter(col("product_category_name").isNotNull()).dropDuplicates(["product_category_name"])
# display(df_order_reviews)


# In[23]:


from pyspark.sql.window import Window


window_spec = Window.partitionBy("order_id").orderBy(
    col("review_creation_date").desc(),
    col("review_answer_timestamp").desc()
)

df_order_reviews = (
    df_order_reviews
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)


# In[25]:


display(
    df_order_reviews
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)


# In[26]:


df_order_reviews.write.mode("overwrite").saveAsTable("silver_order_reviews")

