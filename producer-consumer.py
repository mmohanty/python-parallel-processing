import time
import threading
import concurrent.futures
import queue
import schedule

# Shared resources
data_queue = queue.Queue()  # Thread-safe queue
data_dict = {}  # Dictionary to track items and their timestamps
locks_dict = {}  # Dictionary to store per-item locks and their last-used timestamps
processing_dict = {}  # Dictionary to track start time of currently processed items

PROCESS_TIMEOUT = 20 * 60  # 20 minutes in seconds
LOCK_EXPIRATION = 10 * 60  # 10 minutes in seconds
PROCESSING_TIME_LIMIT = 5 * 60  # 5 minutes in seconds (Consumer max processing time)

dict_lock = threading.Lock()  # Lock for managing shared dictionaries

def get_item_lock(item_id):
    """Returns the lock for a given item_id, creating it if necessary."""
    with dict_lock:
        if item_id not in locks_dict:
            locks_dict[item_id] = {"lock": threading.Lock(), "last_used": time.time()}
        else:
            locks_dict[item_id]["last_used"] = time.time()  # Update last used time
    return locks_dict[item_id]["lock"]

def producer():
    """Produces unique items and adds them to the queue with timestamps."""
    for i in range(1, 11):  # Producing items from 1 to 10
        time.sleep(1)  # Simulate production delay
        with dict_lock:
            if i not in data_dict:
                timestamp = time.time()
                data_dict[i] = timestamp  # Store with timestamp
                data_queue.put(i)
                get_item_lock(i)  # Ensure a lock exists for the item
                print(f"Producer produced: {i}")
            else:
                print(f"Producer skipped (duplicate): {i}")

def consumer():
    """Consumes items from the queue and removes them from the dictionary."""
    while not data_queue.empty():
        try:
            item = data_queue.get(timeout=10)  # Timeout prevents infinite blocking
            lock = get_item_lock(item)  # Get the lock for the item

            with lock:
                with dict_lock:
                    processing_dict[item] = time.time()  # Mark start time
                print(f"Consumer started processing: {item}")

                time.sleep(2)  # Simulate processing delay

                with dict_lock:
                    if item in data_dict:
                        del data_dict[item]  # Remove item after processing
                        del locks_dict[item]  # Cleanup lock
                        del processing_dict[item]  # Remove from tracking

                print(f"Consumer finished processing: {item}")

            data_queue.task_done()

        except queue.Empty:
            print("Consumer: No more data to process.")
            break

def watchdog():
    """Checks for items that haven't been processed within the timeout."""
    while True:
        time.sleep(60)  # Check every 1 minute
        current_time = time.time()

        with dict_lock:
            expired_items = [k for k, v in data_dict.items() if current_time - v > PROCESS_TIMEOUT]
            for item in expired_items:
                print(f"Watchdog: Requeuing stale item {item}")
                data_dict[item] = time.time()  # Reset timestamp
                data_queue.put(item)  # Requeue item

            # Check for consumers that took too long
            stuck_items = [k for k, v in processing_dict.items() if current_time - v > PROCESSING_TIME_LIMIT]
            for item in stuck_items:
                print(f"Watchdog: Consumer exceeded time limit for {item}, releasing lock and requeuing.")
                if item in locks_dict:
                    del locks_dict[item]  # Remove lock
                del processing_dict[item]  # Remove from tracking
                data_queue.put(item)  # Requeue the item for another consumer

def lock_cleaner():
    """Periodically removes unused locks after the expiration period."""
    while True:
        time.sleep(60)  # Check every 1 minute
        current_time = time.time()

        with dict_lock:
            expired_locks = [k for k, v in locks_dict.items() if current_time - v["last_used"] > LOCK_EXPIRATION]
            for item in expired_locks:
                print(f"Lock Cleaner: Removing unused lock for item {item}")
                del locks_dict[item]  # Remove expired lock

def schedule_task():
    """Runs scheduled tasks using the `schedule` library."""
    while True:
        schedule.run_pending()
        time.sleep(1)

# Create a ThreadPoolExecutor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Schedule a consumer to run every 1 minute
schedule.every(1).minutes.do(lambda: executor.submit(consumer))

# Start monitoring threads
watchdog_thread = threading.Thread(target=watchdog, daemon=True)  # Daemon thread for stale item check
lock_cleaner_thread = threading.Thread(target=lock_cleaner, daemon=True)  # Daemon thread for lock cleanup
schedule_thread = threading.Thread(target=schedule_task, daemon=True)  # Schedule runner thread

# Start background threads
watchdog_thread.start()
lock_cleaner_thread.start()
schedule_thread.start()

# Submit producer task
executor.submit(producer)

# Wait until all tasks are completed
data_queue.join()

print("All tasks completed.")
