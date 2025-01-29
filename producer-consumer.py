import threading
import queue
import time

# Shared resources
data_queue = queue.Queue()  # Thread-safe queue
data_dict = {}  # Dictionary to track items and their timestamps
locks_dict = {}  # Dictionary to hold locks for each item and their last usage timestamps
PROCESS_TIMEOUT = 20 * 60  # 20 minutes in seconds
LOCK_EXPIRATION = 10 * 60  # 10 minutes in seconds


def get_item_lock(item_id):
    """Returns the lock for a given item_id, creating it if necessary."""
    with threading.Lock():  # Ensure thread safety while creating a new lock
        if item_id not in locks_dict:
            locks_dict[item_id] = {"lock": threading.Lock(), "last_used": time.time()}
        else:
            locks_dict[item_id]["last_used"] = time.time()  # Update timestamp
    return locks_dict[item_id]["lock"]


def producer():
    """Produces unique items and adds them to the queue with timestamps."""
    for i in range(1, 11):  # Producing items from 1 to 10
        time.sleep(1)  # Simulate production delay
        with threading.Lock():  # Protects dictionary update
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
    while True:
        try:
            item = data_queue.get(timeout=10)  # Timeout prevents infinite blocking
            lock = get_item_lock(item)  # Get the lock for the item

            with lock:  # Lock the item-specific lock
                print(f"Consumer started processing: {item}")
                time.sleep(2)  # Simulate processing delay

                with threading.Lock():  # Protect data_dict modification
                    if item in data_dict:
                        del data_dict[item]  # Remove item after processing
                        del locks_dict[item]  # Cleanup lock

                print(f"Consumer finished processing: {item}")

            data_queue.task_done()

        except queue.Empty:
            print("Consumer exiting: No more data.")
            break


def watchdog():
    """Checks for items that haven't been processed within the timeout."""
    while True:
        time.sleep(60)  # Check every 1 minute
        current_time = time.time()

        with threading.Lock():  # Lock dictionary access
            expired_items = [k for k, v in data_dict.items() if current_time - v > PROCESS_TIMEOUT]
            for item in expired_items:
                print(f"Watchdog: Requeuing stale item {item}")
                data_dict[item] = time.time()  # Reset timestamp
                data_queue.put(item)  # Requeue item


def lock_cleaner():
    """Periodically removes unused locks after the expiration period."""
    while True:
        time.sleep(60)  # Check every 1 minute
        current_time = time.time()

        with threading.Lock():
            expired_locks = [k for k, v in locks_dict.items() if current_time - v["last_used"] > LOCK_EXPIRATION]
            for item in expired_locks:
                print(f"Lock Cleaner: Removing unused lock for item {item}")
                del locks_dict[item]  # Remove expired lock


# Creating threads
producer_thread = threading.Thread(target=producer)
consumer_thread = threading.Thread(target=consumer)
watchdog_thread = threading.Thread(target=watchdog, daemon=True)  # Daemon thread runs in the background
lock_cleaner_thread = threading.Thread(target=lock_cleaner, daemon=True)  # Daemon thread for cleaning up locks

# Start threads
producer_thread.start()
consumer_thread.start()
watchdog_thread.start()
lock_cleaner_thread.start()

# Wait for producer and consumer to finish
producer_thread.join()
consumer_thread.join()

print("All tasks completed.")
