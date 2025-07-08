from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit
import argparse

def create_spark_session(app_name: str = "BronzeBatchIngestion"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_raw_csv(spark, input_path):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

def add_metadata(df, source_name):
    return df.withColumn("source_file", input_file_name()) \
             .withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source", lit(source_name))

def write_to_bronze(df, output_path):
    df.write.mode("append").parquet(output_path)

def process_sources(spark, config_path):
    print(f"Reading JSON config from: {config_path}")
    
    config_df = spark.read.option("multiline", "true").json(config_path)
    config_list = config_df.collect()

    for row in config_list:
        source = row["source_name"]
        input_path = row["input_path"]
        output_path = row["output_path"]

        print(f"\nProcessing: {source}")
        print(f"Reading from: {input_path}")
        df = read_raw_csv(spark, input_path)
        df = add_metadata(df, source)

        print(f"Writing to: {output_path}")
        write_to_bronze(df, output_path)

def main(config_path):
    print("Landing zone to Raw zone Batch ingestion Started.")
    spark = create_spark_session()
    process_sources(spark, config_path)
    spark.stop()
    print("Landing zone to Raw zone Batch ingestion completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True, help="GCS path to JSON config")
    args = parser.parse_args()
    main(args.config_path)