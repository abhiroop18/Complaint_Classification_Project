
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Read Parquet Files").getOrCreate()

# Specify the path to the folder containing the Parquet files
folder_path = "finance_artifact/data_ingestion/feature_store/finance_complaint"

# Read all Parquet files in the folder into a DataFrame
df = spark.read.parquet(folder_path)

top_10_rows = df.limit(10)

# Convert the top 10 rows to a Pandas DataFrame
top_10_pandas_df = top_10_rows.toPandas()

# Display the top 10 rows as a Pandas DataFrame
print(top_10_pandas_df.head(10))


# Show the DataFrame to verify the data

# Stop the Spark session when you're done
spark.stop()
