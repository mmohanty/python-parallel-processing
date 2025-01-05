Apache Flink is a powerful framework for distributed stream and batch data processing. PyFlink allows you to write Flink jobs in Python, providing support for large-scale file processing. Below is a step-by-step example demonstrating how to use PyFlink to read, filter, and process data from a CSV file.

### Installation
#### Install PyFlink:
```
pip install apache-flink
```

To set up a local Flink cluster:

Download Flink: https://flink.apache.org/downloads.html
Start the Flink job manager:

```
./bin/start-cluster.sh
```

Sample PyFlink Code for CSV File Processing

Here is a script that reads a large CSV file, filters records, aggregates data, and writes the output to another file.


PyFlink Batch Job

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import RuntimeContext
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.udf import udf
from pyflink.table import TableConfig

# Initialize Stream Execution Environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)

# Table Environment
table_env = StreamTableEnvironment.create(env)

# Path to the large CSV file
file_path = "large_dataset.csv"
output_file_path = "filtered_output.csv"

# Register input and output formats
table_env.connect(FileSystem().path(file_path)) \
    .with_format(OldCsv()
                 .field("category", DataTypes.STRING())
                 .field("status", DataTypes.STRING())
                 .field("amount", DataTypes.DOUBLE())) \
    .with_schema(Schema()
                 .field("category", DataTypes.STRING())
                 .field("status", DataTypes.STRING())
                 .field("amount", DataTypes.DOUBLE())) \
    .create_temporary_table("input_table")

table_env.connect(FileSystem().path(output_file_path)) \
    .with_format(OldCsv()
                 .field("category", DataTypes.STRING())
                 .field("total_amount", DataTypes.DOUBLE())) \
    .with_schema(Schema()
                 .field("category", DataTypes.STRING())
                 .field("total_amount", DataTypes.DOUBLE())) \
    .create_temporary_table("output_table")

# Query to process data
query = """
    SELECT category, SUM(amount) AS total_amount
    FROM input_table
    WHERE status = 'Completed'
    GROUP BY category
"""

# Run the query
result_table = table_env.sql_query(query)

# Write result to CSV
result_table.execute_insert("output_table").wait()

print(f"\nFiltered data written to {output_file_path}")

```

Running the Script

Ensure the Flink cluster is running:
```
./bin/start-cluster.sh
```

Run the PyFlink script:

```
python pyflink_job.py
```

