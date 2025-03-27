from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, avg, rank
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder\
    .appName("GridDensityCalculator")\
    .config("spark.sql.shuffle.partitions", "200")\
    .config("spark.default.parallelism", "200")\
    .getOrCreate()

# Read and prepare points
points = spark.read.option("header", "false").csv("hdfs://localhost:9000/user/Project3/data/points.csv")\
    .toDF("x", "y")\
    .withColumn("x", col("x").cast("double"))\
    .withColumn("y", col("y").cast("double"))  # .filter((col("x").between(1, 10000)) & (col("y").between(1, 10000))) 

# Dynamic repartitioning
num_partitions = max(200, int(points.count() / 50000))
points = points.repartition(num_partitions).cache()

# Compute grid cell ID (500 cols, 20x20 cells)
points = points.withColumn("row", floor((col("y") - 1) / 20))\
               .withColumn("col", floor((col("x") - 1) / 20))\
               .withColumn("cell_id", (col("row") * 500 + col("col")))

# Count points per grid cell
cell_counts = points.groupBy("cell_id").count().withColumnRenamed("count", "cell_count").cache()

# Add row and col for neighbor calculation
cell_counts = cell_counts.withColumn("row", floor(col("cell_id") / 500))\
                         .withColumn("col", col("cell_id") % 500)
                         
cell_counts.show()

# Define neighbor offsets (8 possible surrounding cells)
offsets = spark.createDataFrame([
    (-1, -1), (-1, 0), (-1, 1),
    (0, -1),           (0, 1),
    (1, -1),  (1, 0),  (1, 1)
], ["row_offset", "col_offset"]).cache()

# Generate neighbor cell_ids with boundary checks using crossJoin
cell_counts_with_neighbors = cell_counts.crossJoin(offsets)\
    .withColumn("neighbor_row", col("row") + col("row_offset"))\
    .withColumn("neighbor_col", col("col") + col("col_offset"))\
    .filter((col("neighbor_row").between(0, 499)) & (col("neighbor_col").between(0, 499)))\
    .withColumn("neighbor_cell_id", col("neighbor_row") * 500 + col("neighbor_col"))

# Join to get neighbor counts and compute average
neighbors = cell_counts_with_neighbors.join(
    cell_counts.select("cell_id", "cell_count")\
               .withColumnRenamed("cell_id", "neighbor_cell_id")\
               .withColumnRenamed("cell_count", "neighbor_count"),
    "neighbor_cell_id",
    "left_outer"
).groupBy("cell_id", "cell_count")\
 .agg(avg(col("neighbor_count")).alias("neighbor_avg"))

# Compute relative density
relative_density = neighbors.withColumn("I_X", col("cell_count") / col("neighbor_avg"))

# Get top 50
window_spec = Window.orderBy(col("I_X").desc())
top50 = relative_density.withColumn("rank", rank().over(window_spec))\
                        .filter("rank <= 50")\
                        .select("cell_id", "I_X")

# Show top 50 results
top50.show()



# --- Problem 2 - 3: Report Neighbor Details for Top 50 ---
# Add row and col to top50
top50_with_points = top50.withColumn("row", floor(col("cell_id") / 500))\
                         .withColumn("col", col("cell_id") % 500)

# Generate neighbor cell_ids for top 50
top50_neighbors = top50_with_points.crossJoin(offsets)\
    .withColumn("neighbor_row", col("row") + col("row_offset"))\
    .withColumn("neighbor_col", col("col") + col("col_offset"))\
    .filter((col("neighbor_row").between(0, 499)) & (col("neighbor_col").between(0, 499)))\
    .withColumn("neighbor_cell_id", col("neighbor_row") * 500 + col("neighbor_col"))

# Join with relative_density to get neighbor I_X values
top50_neighbor_details = top50_neighbors.join(
    relative_density.select("cell_id", "I_X").withColumnRenamed("cell_id", "neighbor_cell_id")\
                                            .withColumnRenamed("I_X", "neighbor_I_X"),
    "neighbor_cell_id",
    "left_outer"
).select(
    col("cell_id").alias("top_cell_id"),
    col("I_X").alias("top_cell_I_X"),
    col("neighbor_cell_id"),
    col("neighbor_I_X")
).orderBy("top_cell_I_X", descending=True)

# Show results
print("Top 50 Grid Cells with Neighbor Details:")
top50_neighbor_details.show(400, truncate=False)  # Show up to 400 rows to cover all neighbors (50 cells * 8 max neighbors)

spark.stop()