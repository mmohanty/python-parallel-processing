
    ```python
        pip install celery pandas
        pip install redis
        docker run -p 6379:6379 -d redis
    ```

1. Celery Configuration (celeryconfig.py)
    ```python
   # celeryconfig.py
    broker_url = "redis://localhost:6379/0"  # Redis broker
    result_backend = "redis://localhost:6379/0"  # Redis backend for result storage
   ```
2. Define Celery Tasks (tasks.py)
```python
from celery import Celery
import pandas as pd

# Create a Celery app and configure it
celery_app = Celery("file_processing", include=["tasks"])
celery_app.config_from_object("celeryconfig")

@celery_app.task
def process_csv(file_path):
    """ Task to process CSV file asynchronously """
    # Load CSV file using pandas (can be extended to Dask if needed)
    df = pd.read_csv(file_path)

    # Example 1: Filter rows where 'status' is 'Completed'
    filtered_df = df[df["status"] == "Completed"]

    # Example 2: Perform aggregation (group by category and sum amount)
    agg_result = filtered_df.groupby("category")["amount"].sum()

    # Save filtered data
    output_file = "filtered_output.csv"
    filtered_df.to_csv(output_file, index=False)

    return {
        "message": "CSV processed successfully",
        "filtered_rows": len(filtered_df),
        "aggregated_result": agg_result.to_dict(),
        "output_file": output_file,
    }

```
3. Main Script to Trigger Task (app.py)
```python
from tasks import process_csv

if __name__ == "__main__":
    # Path to large CSV file
    file_path = "large_dataset.csv"

    # Trigger the Celery task
    result = process_csv.delay(file_path)

    # Wait for the task result
    print("Task submitted... waiting for the result.")
    output = result.get(timeout=30)  # Timeout of 30 seconds

    # Display result
    print("\nTask completed!")
    print(output)

```
4. Run Celery Worker
```
celery -A tasks worker --loglevel=info
```
5. Run the Script to Trigger the Task
```
python app.py
```
   