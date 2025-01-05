from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Create a Spark Session
    spark = SparkSession.builder \
        .appName("PySpark CSV Processing Project") \
        .getOrCreate()

    # Define input and output paths
    input_path = "data/sample.csv"
    output_path = "output/processed_data/"

    # Read CSV file into a DataFrame
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # Show the input data
    print("Input Data:")
    df.show()

    # Perform a simple transformation: Select specific columns and filter by age > 30
    processed_df = df.select("id", "name", "age", "city").filter(col("age") > 30)

    print("Processed Data:")
    processed_df.show()

    # Save the processed DataFrame as CSV in the output folder
    processed_df.write.option("header", True).mode("overwrite").csv(output_path)

    print(f"Processed data saved to {output_path}")

    # Stop the Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
