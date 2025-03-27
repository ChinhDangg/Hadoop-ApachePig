from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, min, max, count


# Initialize Spark session
spark = SparkSession.builder.appName("TransactionFiltering").getOrCreate()

file_path = "hdfs://localhost:9000/user/Project1/data/transactions.csv"

# Read the CSV file into a DataFrame without a header
T = spark.read.option("header", "false").csv(file_path)

# Manually assign column names to the DataFrame
T = T.toDF("transId", "customerId", "transTotal", "transNumItems", "transDescription")

# Task 1: Filter transactions with transTotal >= 200
T1 = T.filter(col("transTotal") >= 200)

# Task 2: Group by transNumItems and calculate required aggregations
T2 = T1.groupBy("transNumItems").agg(
    sum("transTotal").alias("sum_transTotal"),
    avg("transTotal").alias("avg_transTotal"),
    min("transTotal").alias("min_transTotal"),
    max("transTotal").alias("max_transTotal")
)

# Show T2 result
T2.show()


# Task 3: Group by customerId and count transactions
T3 = T1.groupBy("customerId").agg(
    count("transId").alias("transaction_count_T3")
)

# Show T3 result
T3.show()

# Task 4: Filter transactions with transTotal >= 600
T4 = T.filter(col("transTotal") >= 600)


# Task 5: Group by customerId and count transactions for T4
T5 = T4.groupBy("customerId").agg(
    count("transId").alias("transaction_count_T5")
)

# Show T5 result
T5.show()


# Task 6: Join T5 and T3 on customerId and apply the condition
T6 = T5.join(T3, "customerId") \
    .filter((col("transaction_count_T5") * 5) < col("transaction_count_T3"))

# Show T6 result
T6.show()
