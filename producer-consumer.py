import threading
import queue
import time

# Shared resources
data_queue = queue.Queue()  # Thread-safe queue
data_dict = {}  # Dictionary to track items and their timestamps
lock = threading.Lock()  # Lock for thread safety
PROCESS_TIMEOUT = 20 * 60  # 20 minutes in seconds

def producer():
    """Produces unique items and adds them to the queue with timestamps."""
    for i in range(1, 11):  # Producing items from 1 to 10
        time.sleep(1)  # Simulate production delay
        with lock:
            if i not in data_dict:
                timestamp = time.time()
                data_dict[i] = timestamp  # Store with timestamp
                data_queue.put(i)
                print(f"Producer produced: {i}")
            else:
                print(f"Producer skipped (duplicate): {i}")

def consumer():
    """Consumes items from the queue and removes them from the dictionary."""
    while True:
        try:
            item = data_queue.get(timeout=10)  # Timeout prevents infinite blocking
            print(f"Consumer started processing: {item}")
            time.sleep(2)  # Simulate processing delay
            with lock:
                if item in data_dict:
                    del data_dict[item]  # Remove item after processing
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
        with lock:
            expired_items = [k for k, v in data_dict.items() if current_time - v > PROCESS_TIMEOUT]
            for item in expired_items:
                print(f"Watchdog: Requeuing stale item {item}")
                data_dict[item] = time.time()  # Reset timestamp
                data_queue.put(item)  # Requeue item

# Creating threads
producer_thread = threading.Thread(target=producer)
consumer_thread = threading.Thread(target=consumer)
watchdog_thread = threading.Thread(target=watchdog, daemon=True)  # Daemon thread runs in the background

# Start threads
producer_thread.start()
consumer_thread.start()
watchdog_thread.start()

# Wait for producer and consumer to finish
producer_thread.join()
consumer_thread.join()

print("All tasks completed.")
