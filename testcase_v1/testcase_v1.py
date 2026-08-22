import multiprocessing
import random
import time
import pymysql
import os
import threading
from datetime import datetime

# --- Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'msandbox',
    'password': 'msandbox',
    'database': 'vt_byuser'
}

NUM_WORKER_PROCESSES = 20  
BATCH_SIZE = 150 # the number of values in the query

# Print a progress update every N queries.
REPORT_INTERVAL = 10000

# Directory to store periodic SHOW GLOBAL STATUS dumps (one subfolder per run)
STATUS_OUTPUT_BASEDIR = os.path.join(os.path.dirname(__file__), "status_results")
# Interval (seconds) between SHOW GLOBAL STATUS snapshots
STATUS_INTERVAL_SECONDS = 10

def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def safe_print(lock, message):
    """
    A process-safe print function that uses a shared lock.
    """
    with lock:
        print(message, flush=True)


# Background thread to collect SHOW GLOBAL STATUS periodically
def collect_global_status_periodically(dump_dir, interval_seconds, stop_event, print_lock):
    """
    Connects to MySQL and appends SHOW GLOBAL STATUS snapshots to a file
    every `interval_seconds` seconds until `stop_event` is set.
    """
    _ensure_dir(dump_dir)
    out_path = os.path.join(dump_dir, "global_status.log")
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        safe_print(print_lock, f"[StatusCollector] Writing to {out_path} (every {interval_seconds}s)")
        with open(out_path, "a", encoding="utf-8") as fh:
            while not stop_event.is_set():
                ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                try:
                    cur = conn.cursor()
                    cur.execute("SHOW GLOBAL STATUS")
                    rows = cur.fetchall()  # list[(name, value)]
                    cur.close()
                except Exception as e:
                    safe_print(print_lock, f"[StatusCollector] Error collecting status: {e}")
                    # Back off a bit before next try
                    if stop_event.wait(interval_seconds):
                        break
                    continue

                # Write timestamp and snapshot
                fh.write(ts + "\n")
                for name, value in rows:
                    fh.write(f"{name}\t{value}\n")
                fh.write("\n")
                fh.flush()

                if stop_event.wait(interval_seconds):
                    break
    finally:
        if conn:
            conn.close()
            safe_print(print_lock, "[StatusCollector] Connection closed.")

# 1. The function to gather all in_values (run by the main process)
def gather_all_in_values():
    """Connects to the DB, queries for all distinct k's, and returns them as a list."""
    print("[Main Process] Starting to fetch all distinct channel_id ...")
    in_values = []
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("select distinct channel_id from group_members;")
        in_values = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"[Main Process] Finished. Found {len(in_values)} total channel_id.")
    except pymysql.MySQLError as err:
        print(f"[Main Process] Error: {err}")
    finally:
        if conn:
            conn.close()
            print("[Main Process] Connection closed.")
    return in_values

# 2. The target function for the worker processes
def worker_query_data(process_id, in_values, print_lock, num_queries=None):
    """
    Establishes a single connection and runs a random query in a loop.
    This function is executed in a separate process.
    """
    if num_queries is None:
        safe_print(print_lock, f"[Worker {process_id}] Process started, will run indefinitely.")
    else:
        safe_print(print_lock, f"[Worker {process_id}] Process started, will run {num_queries} queries.")
    
    conn = None
    total_records_found = 0
    start_time = time.time()

    try:
        # Each process establishes its own connection to the database
        conn = pymysql.connect(**DB_CONFIG)
        safe_print(print_lock, f"[Worker {process_id}] Connection established.")

        # Main loop to run the query repeatedly
        count = 0
        while num_queries is None or count < num_queries:
            # 3. generate the random value in query
            random_values = random.sample(in_values, BATCH_SIZE)

            # The placeholder for PyMySQL is %s
            placeholders = ', '.join(['%s'] * len(random_values))
            # Using the query from your script
            query = f"SELECT user_id, channel_id, channel_tenant_id, date_joined, date_deleted, last_read, last_read_abs, is_open, channel_type, channel_privacy_type, share_type, target_user_id, target_user_tenant_id, is_target_user_deleted, user_tenant_id, is_starred, latest_counted_ts, latest_event_ts, history_invalid_ts, max_invalid_message_ts, date_archived FROM group_members WHERE channel_id IN ({placeholders}) and date_deleted = 0 and channel_type = 0 limit 10000, 1"
            
            cursor = conn.cursor()
            cursor.execute(query, tuple(random_values))
            results = cursor.fetchall()
            cursor.close()
            
            count += 1
            if num_queries is not None and (count % REPORT_INTERVAL == 0):
                safe_print(print_lock, f"[Worker {process_id}] Progress: {count}/{num_queries} queries completed.")
            elif num_queries is None and (count % REPORT_INTERVAL == 0):
                safe_print(print_lock, f"[Worker {process_id}] Progress: {count} queries completed (no limit).")

    except pymysql.MySQLError as err:
        safe_print(print_lock, f"[Worker {process_id}] Error: {err}")
    except Exception as e:
        safe_print(print_lock, f"[Worker {process_id}] An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed when the process finishes or errors out
        if conn:
            conn.close()
            end_time = time.time()
            duration = end_time - start_time
            safe_print(
                print_lock,
                f"[Worker {process_id}] FINISHED. Ran {count if num_queries is None else num_queries} queries "
            )


if __name__ == "__main__":
    # --- STEP 1: The main process gathers all channel_id first ---
    print("\n--- Starting Step 1: Gather All in_values  ---")
    all_in_values = gather_all_in_values()
    print("--- Step 1 Complete ---\n")

    # Prepare status dump directory for this run and start the collector thread
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    status_run_dir = os.path.join(STATUS_OUTPUT_BASEDIR, run_stamp)
    stop_event = threading.Event()

    if not all_in_values:
        print("No data_ids found. Exiting.")
        exit()

    # --- STEP 2 & 3: Create and start worker processes ---
    print(f"--- Starting Step 2: Creating {NUM_WORKER_PROCESSES} worker processes ---")
    
    # Create a lock to be shared among processes for safe printing
    lock = multiprocessing.Lock()

    status_thread = threading.Thread(
        target=collect_global_status_periodically,
        args=(status_run_dir, STATUS_INTERVAL_SECONDS, stop_event, lock),
        daemon=True,
    )
    status_thread.start()
    
    worker_processes = []
    for i in range(NUM_WORKER_PROCESSES):
        # Create a Process, not a Thread
        process = multiprocessing.Process(
            target=worker_query_data, 
            args=(i + 1, all_in_values, lock, None) # Pass the lock to the worker and num_queries=None
        )
        worker_processes.append(process)
        process.start() # Start the process

    # Wait for all worker processes to complete their execution
    for process in worker_processes:
        process.join()

    print("\n--- All worker processes have finished their loops. ---")

    # Signal the status collector to stop and wait for it to finish
    stop_event.set()
    status_thread.join(timeout=10)
