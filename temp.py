from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Recursive Parquet Read") \
    .getOrCreate()

# HDFS root folder
hdfs_folder = "hdfs:///path/to/hdfs/folder"

# Initialize Hadoop FileSystem
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
Path = spark._jvm.org.apache.hadoop.fs.Path

# Recursive function to find all parquet file paths
def list_parquet_files(path):
    parquet_files = []
    file_status = fs.listStatus(Path(path))
    
    for status in file_status:
        p = status.getPath()
        if status.isDirectory():
            parquet_files.extend(list_parquet_files(p.toString()))
        elif p.getName().endswith(".parquet"):
            parquet_files.append(p.toString())
    
    return parquet_files

# Get all parquet file paths recursively
all_parquet_files = list_parquet_files(hdfs_folder)

# Read and concatenate all into one big pandas DataFrame
pandas_dfs = []
for file_path in all_parquet_files:
    print(f"Reading: {file_path}")
    spark_df = spark.read.parquet(file_path)
    pandas_df = spark_df.toPandas()
    pandas_dfs.append(pandas_df)

# Concatenate all into one DataFrame
final_df = pd.concat(pandas_dfs, ignore_index=True)

# Done
print(final_df.shape)