 <!-- TOC -->
* [Apache Spark (PySpark)](#apache-spark-pyspark)
    * [Strengths:](#strengths-)
    * [Ideal for:](#ideal-for-)
  * [Architecture Diagram](#architecture-diagram)
  * [File Processing Workflow](#file-processing-workflow)
  * [Use Case](#use-case)
  * [Monitoring and Debugging](#monitoring-and-debugging)
  * [Notes](#notes)
  * [Alternative Frameworks for Specific Use Cases](#alternative-frameworks-for-specific-use-cases)
    * [Dask](#dask)
    * [Ray](#ray)
    * [Celery](#celery)
<!-- TOC -->

# Apache Spark (PySpark)

### Strengths: 
Highly scalable, built-in support for file-based data (CSV, JSON, Parquet, etc.), fault-tolerant.

### Ideal for: 
Large-scale distributed data processing.

Based on the need for large-scale file processing, Apache Spark is an excellent choice due to its distributed architecture, fault tolerance, and ease of scaling.

## Architecture Diagram

                    +----------------+
                    |    Client       |
                    +----------------+
                           |
                           |
                  +------------------+
                  |    Spark Driver   |
                  +------------------+
                           |
          +------------------------------------+
          |                                    |
      +------------------+               +------------------+
      |    Worker Node   |               |    Worker Node   |
      +------------------+               +------------------+
              |                                    |
       +--------------+                     +--------------+
       | Task 1       |                     | Task 2       |
       | File Part A  |                     | File Part B  |
       +--------------+                     +--------------+
  

## File Processing Workflow

Here is a general workflow for processing files and extracting data:

Read files: Read CSV, JSON, or other formats.

Transformation: Apply filters, extract columns, or parse fields.

Extraction: Store or output processed data.


## Use Case

Process thousands of files across multiple nodes.

Extract relevant columns and perform grouping.

Output data for further downstream processing (e.g., reports, ML).

## Monitoring and Debugging

Spark UI: Access the Spark Web UI to monitor tasks.


## Notes
If native hadoop library is not found in the target platform, it tries to read builtin-java classes
**WARN** NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


## Alternative Frameworks for Specific Use Cases

* ### Dask
  * Great for Python-native parallel processing.
  * Sample Code
  ```python
    import dask.dataframe as dd

    # Read CSV file using Dask (parallelized)
    file_path = "large_dataset.csv"  # Replace with your file path
    df = dd.read_csv(file_path)
    
    # Display the structure of the dataframe
    print("Columns in CSV file:", df.columns)
    
    # Example 1: Extract specific information (e.g., filtering rows)
    filtered_df = df[df["status"] == "Completed"]  # Filtering where 'status' is 'Completed'
    print("Filtered DataFrame: Completed status")
    print(filtered_df.head(5).compute())  # Compute and display the first 5 rows
    
    # Example 2: Perform Aggregation (group by)
    agg_df = df.groupby("category")["amount"].sum().compute()
    print("\nTotal sum of 'amount' by 'category':")
    print(agg_df)
    
    # Example 3: Extract rows where value in a specific column exceeds a threshold
    high_value_df = df[df["amount"] > 1000]  # Amount greater than 1000
    print("\nRows with amount > 1000:")
    print(high_value_df.head(5).compute())
    
    # Example 4: Save filtered data to a new CSV file
    output_file_path = "filtered_dataset.csv"
    filtered_df.to_csv(output_file_path, single_file=True)  # Save as a single CSV file
    print(f"\nFiltered data saved to {output_file_path}")
    
    # Closing statement
    print("\nDask DataFrame processing completed successfully!")

    ```

    * ### Ray
      * Ideal for workflows with complex distributed functions.
      * ```python
        import ray
        ray.init()
        
        @ray.remote
        def process_file_chunk(chunk):
            # Simulate processing
            return len(chunk)
        
        chunks = ["chunk1", "chunk2", "chunk3"]
        results = ray.get([process_file_chunk.remote(chunk) for chunk in chunks])
        print(results)
        ray.shutdown()
        ```
    
    * ### Celery
      * Suited for task queues and background jobs.
      * [Sample Code](README_Celery.md)
