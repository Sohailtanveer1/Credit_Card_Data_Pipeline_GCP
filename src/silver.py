from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import json

def create_spark_session(app_name="SilverLayerProcessing"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_from_bronze(spark, input_path):
    return spark.read.parquet(input_path)

def clean_dataframe(df, rules):
    # Rename columns
    if "rename_columns" in rules and rules["rename_columns"]:
        rename_dict = rules["rename_columns"]
        if isinstance(rename_dict, dict):
            pass  # already a dictionary
        else:
            rename_dict = rename_dict.asDict()  # convert Row to dict

        for old_col, new_col in rename_dict.items():
            if new_col:  # skip if new_col is None or empty
                df = df.withColumnRenamed(old_col, new_col)

    # Cast columns to required types
    if "cast_columns" in rules and rules["cast_columns"]:
        cast_dict = rules["cast_columns"]
        if isinstance(cast_dict, dict):
            pass
        else:
            cast_dict = cast_dict.asDict()

        for col_name, data_type in cast_dict.items():
            if data_type:
                df = df.withColumn(col_name, col(col_name).cast(data_type))

    # Drop rows with nulls in required columns
    if "drop_nulls" in rules and rules["drop_nulls"]:
        drop_nulls_list = rules["drop_nulls"]
        if isinstance(drop_nulls_list, list):
            df = df.dropna(subset=drop_nulls_list)

    return df

def write_to_silver(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

def process_sources(spark, config_path):
    config_df = spark.read.option("multiline", "true").json(config_path)
    configs = config_df.collect()

    for row in configs:
        row_dict = row.asDict()
        source = row_dict["source_name"]
        print(f"\nProcessing source: {source}")

        input_path = row_dict["input_path"]
        output_path = row_dict["output_path"]

        print(f"Reading from: {input_path}")
        df = read_from_bronze(spark, input_path)

        print("Cleaning and structuring...")
        df_cleaned = clean_dataframe(df, row_dict)

        print(f"Writing to: {output_path}")
        write_to_silver(df_cleaned, output_path)

def main(config_path):
    print("Silver layer processing Started.")
    spark = create_spark_session()
    process_sources(spark, config_path)
    spark.stop()
    print("Silver layer processing completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True, help="GCS path to JSON config for silver layer")
    args = parser.parse_args()
    main(args.config_path)
