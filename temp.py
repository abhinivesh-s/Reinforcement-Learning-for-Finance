from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Read Parquet Iteratively to Pandas") \
    .getOrCreate()

# HDFS path
hdfs_folder = "hdfs:///path/to/hdfs/folder"

# Access Hadoop FileSystem
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_folder)
file_status = fs.listStatus(path)

# Collect all individual Pandas DataFrames
pandas_dfs = []

for status in file_status:
    file_path = status.getPath().toString()
    if file_path.endswith(".parquet"):
        print(f"Reading: {file_path}")
        spark_df = spark.read.parquet(file_path)
        pandas_df = spark_df.toPandas()
        pandas_dfs.append(pandas_df)

# Concatenate into one big DataFrame
final_df = pd.concat(pandas_dfs, ignore_index=True)

# Done: final_df is your full pandas dataframe
print(final_df.shape)