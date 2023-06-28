from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder.appName('sparkAppName').getOrCreate()

input_path = "https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx"

print("reading in input data ...")

# ----------
# i. TC_Data
# ----------

tc_pandas_schema = {
    "TRANS_ID": int,
    "TCTYPE": str,
    "CREATEDAT": str,
    "EXPIREDAT": str,
    "CUSTOMERID": int,
    "ORDERID": float,
    "AMOUNT": float,
    "REASON": str
}

# read in excel file with timestamps as strings
tc_pdf = pd.read_excel(input_path, "TC_Data", header=0, dtype=tc_pandas_schema)

tc_spark_schema = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("tc_type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("expired_at", StringType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_id", FloatType(), True),
    StructField("amount", FloatType(), True),
    StructField("reason", StringType(), True)
])

# convert to a spark dataframe
tc = spark.createDataFrame(tc_pdf, schema = tc_spark_schema)

# clean data and adjust types
tc = tc.select([when(tc[col] == "NaN", None).otherwise(tc[col]).alias(col) for col in tc.columns])
tc = tc \
        .withColumn("created_at", to_timestamp(tc.created_at)) \
        .withColumn("expired_at", to_timestamp(tc.expired_at))


# --------
# ii. Sales
# --------

sales_pdf = pd.read_excel(input_path, "Sales", header=0)
sales = spark.createDataFrame(sales_pdf) \
    .withColumnRenamed("ORDERID", "order_id") \
    .withColumnRenamed("CUSTOMERID", "customer_id") \
    .withColumnRenamed("PREDISCOUNTGROSSPRODUCTSALES", "pre_discount_gross_product_sales") \
    .withColumnRenamed("ORDERWEIGHT", "order_weight")

# ------------
# iii. Customers
# ------------

customers_pdf = pd.read_excel(input_path, "Customers", header=0)
customers = spark.createDataFrame(customers_pdf) \
    .withColumnRenamed("CUSTOMERID", "customer_id") \
    .withColumnRenamed("EMAIL", "email") \
    .withColumnRenamed("FIRSTNAME", "first_name") \
    .withColumnRenamed("BILLINGPOSTCODE", "billing_post_code")


print("-------------------------")
print("1. Task 1 (Data Pipeline)")
print("-------------------------")

# In TC_Data,
# The dataset includes three types of transaction types: earned, spent, and expired. Assign
# TRANS_ID of spent/expired transactions to earned transactions in the newly created
# REDEEMID column, in a FIFO order based on the CREATEDAT date for each customer. Create a
# new column called REDEEMID in the TC_Data table.
#
# Criteria:
# ● For each earned transaction, assign the TRANS_ID of the oldest unassigned spent or
# expired transaction for the same customer.
# ● If there are no unassigned spent or expired transactions for the customer, leave the
# REDEEMID field blank.
# ● The solution should be scalable and efficient for processing large datasets.

# a non-earned txn will signify the boundary of a new partition, so mark them with a 1
t1 = tc.withColumn("not_earned", when(tc.tc_type != "earned", 1).otherwise(0))

# do a cumulative sum over the custom window to correctly mark our partitions
w1 = Window.partitionBy("customer_id").orderBy(t1.created_at.desc())
t1 = t1.withColumn("part", sum("not_earned").over(w1))

# for each partition, we only want the txn id of the latest non-earned txn
# so we can create a smaller table out of our main table to be the other
# side of this join
t1_ = t1 \
        .where(t1.tc_type != "earned") \
        .select([col(x).alias(x + "_") for x in t1.columns])

# display tables before join
t1_.show(truncate=False)
t1.orderBy(t1.customer_id, t1.created_at.desc()).show(truncate=False)

# join where partitions are the same, and customer_ids are the same
# we only want to have a redeem_id for earned transactions
t1 = t1 \
        .join(t1_, (t1.part == t1_.part_) & (t1.customer_id == t1_.customer_id_) & (t1.tc_type == "earned"), "left") \
        .select(
            col("transaction_id"),
            col("tc_type"),
            col("created_at"),
            col("expired_at"),
            col("customer_id"),
            col("order_id"),
            col("amount"),
            col("reason"),
            col("transaction_id_").alias("redeem_id")
        )


# display results
t1.orderBy(t1.customer_id, t1.created_at.desc()).show(truncate=False)

print("------------------------------------------")
print("2. Task 2 (Data Warehouse & Data Modeling)")
print("------------------------------------------")

# Perform a data transformation to join the TC_Data and Sale tables to create a new table that
# shows the customer name, order ID, and amount of each sale.

t2a = sales \
        .join(customers, sales.customer_id == customers.customer_id, "left") \
        .select(
            col("first_name").alias("customer_name"),
            col("order_id"),
            col("pre_discount_gross_product_sales")
        )

# display results
t2a.show(truncate=False)

# join the TC_Data and Customers tables to create a new table that shows the customer name,
# transaction type, and amount for all transactions made by each customer.

t2b = tc \
        .join(customers, tc.customer_id == customers.customer_id, "left") \
        .select(
            col("first_name").alias("customer_name"),
            col("tc_type").alias("transaction_type"),
            col("amount")
        )

# display results
t2b.show(truncate=False)