import time
import random
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from dagster_dag_factory.factory.helpers.streaming import execute_parallel_stream


def test_row_based_parallelism():
    """Simulates a database transfer (SQL Server -> Snowflake)."""
    print("\n--- 🏎️  Testing Row-Based Parallelism (DB Transfer) ---")
    
    def producer(q):
        for i in range(1, 6):
            print(f"[Reader] Fetching chunk {i}...")
            time.sleep(0.5)  # Simulate network latency
            q.put(f"CHVNK_{i}_DATA")
        print("[Reader] All chunks fetched.")

    def consumer(item):
        print(f"[Writer] Writing {item} to target...")
        time.sleep(0.8)  # Simulate Snowflake being slower than SQL Server
        print(f"[Writer] Finished {item}.")

    start = time.time()
    execute_parallel_stream(producer, consumer, num_consumers=1)
    duration = time.time() - start
    print(f"✅ DB Transfer complete in {round(duration, 2)}s")


def test_file_based_parallelism():
    """Simulates SFTP -> S3 with multiple parallel file transfers."""
    print("\n--- 🚀 Testing File-Based Parallelism (SFTP -> S3) ---")
    
    files_to_process = ["data_1.csv", "data_2.csv", "images.zip", "logs_old.txt", "report_v1.pdf"]
    
    def producer(q):
        for filename in files_to_process:
            print(f"[Scanner] Discovered file: {filename}")
            time.sleep(0.2)  # Discovery is fast
            q.put(filename)
        print("[Scanner] Scan complete.")

    def consumer(filename):
        # Multiple consumers working in parallel
        worker_name = threading.current_thread().name
        print(f"[{worker_name}] Starting transfer: {filename}")
        transfer_time = random.uniform(1.0, 3.0)
        time.sleep(transfer_time)
        print(f"[{worker_name}] ✅ Finished {filename} in {round(transfer_time, 2)}s")

    import threading
    start = time.time()
    # Use 3 parallel consumers to process 5 files
    execute_parallel_stream(producer, consumer, num_consumers=3)
    duration = time.time() - start
    print(f"✅ Multi-file transfer complete in {round(duration, 2)}s")


def test_error_propagation():
    """Verified that errors in background threads are caught."""
    print("\n--- ⚠️  Testing Error Propagation ---")
    
    def producer(q):
        q.put("GOOD_DATA")
        time.sleep(0.5)
        print("[Reader] Simulating database crash...")
        raise ValueError("Database connection lost!")

    def consumer(item):
        print(f"[Writer] Processing {item}")

    try:
        execute_parallel_stream(producer, consumer)
    except Exception as e:
        print(f"✅ Successfully caught error: {e}")


if __name__ == "__main__":
    test_row_based_parallelism()
    test_file_based_parallelism()
    test_error_propagation()
