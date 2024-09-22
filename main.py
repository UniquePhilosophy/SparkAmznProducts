import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_replace

def transform_data(data_source: str, output_uri: str) -> None:
    with SparkSession.builder.appName("Amazon Product Data").getOrCreate() as spark:
        df = spark.read.option("header", "true").csv(data_source)

        # Remove commas from integer columns
        df = df.withColumn("no_of_ratings", regexp_replace(col("no_of_ratings"), ",", ""))

        # Remove currency symbols and commas from price columns
        df = df.withColumn("discount_price", regexp_replace(col("discount_price"), "₹|,", ""))
        df = df.withColumn("actual_price", regexp_replace(col("actual_price"), "₹|,", ""))

        # Filter out products with less than 100 reviews
        df_filtered = df.filter(col("no_of_ratings").cast("int") >= 100)

        # Add a new column 'relative_discount' with discount price divided by actual price
        df_filtered = df_filtered.withColumn(
            "relative_discount", expr("cast(discount_price as double) / cast(actual_price as double)")
        )

        # Order by the new 'relative_discount' column (ascending for lowest discount)
        df_filtered = df_filtered.orderBy(col("relative_discount").asc())

        # Log to terminal
        print(f"Number of rows in SQL query: {df_filtered.count()}")

        # Write the transformed data to your S3 bucket
        df_filtered.write.mode("overwrite").parquet(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source')
    parser.add_argument('--output_uri')
    args = parser.parse_args()

    transform_data(args.data_source, args.output_uri)
