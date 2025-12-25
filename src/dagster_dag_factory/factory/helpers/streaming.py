import queue
import threading
from typing import Callable, Any, List, Optional


class StreamError(Exception):
    """Custom error for streaming failures."""
    pass


def execute_parallel_stream(
    producer_fn: Callable[[queue.Queue], Any],
    consumer_fn: Optional[Callable[[Any], Any]] = None,
    worker_fn: Optional[Callable[[queue.Queue], Any]] = None,
    num_consumers: int = 1,
    queue_size: int = 10,
    logger: Optional[Any] = None,
):
    """
    Executes a producer-consumer stream in parallel.
    
    :param producer_fn: Function that populates the queue. Must accept queue as first arg.
    :param consumer_fn: Simple function that processes one item at a time.
    :param worker_fn: Advanced function that takes the queue and manages its own loop.
    :param num_consumers: Number of consumer threads to run.
    :param queue_size: Maximum size of the bounded queue.
    :param logger: Optional logger for errors.
    """
    if not consumer_fn and not worker_fn:
        raise ValueError("Either 'consumer_fn' or 'worker_fn' must be provided.")

    q = queue.Queue(maxsize=queue_size)
    errors = []
    
    def producer_wrapper():
        try:
            producer_fn(q)
        except Exception as e:
            if logger:
                logger.error(f"Producer failed: {e}")
            errors.append(e)
        finally:
            # Signal consumers that production is finished
            for _ in range(num_consumers):
                q.put(None)

    def default_worker(queue_obj):
        while True:
            item = queue_obj.get()
            if item is None:
                queue_obj.task_done()
                break
            consumer_fn(item)
            queue_obj.task_done()

    def consumer_wrapper():
        try:
            target_fn = worker_fn or default_worker
            target_fn(q)
        except Exception as e:
            if logger:
                logger.error(f"Consumer/Worker failed: {e}")
            errors.append(e)
            # Drain the queue to unblock the producer if it's still running
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    break

    # Start threads
    producer_thread = threading.Thread(target=producer_wrapper, name="Producer")
    producer_thread.start()
    
    consumer_threads = []
    for i in range(num_consumers):
        t = threading.Thread(target=consumer_wrapper, name=f"Consumer-{i+1}")
        t.start()
        consumer_threads.append(t)
        
    # Wait for completion
    producer_thread.join()
    for t in consumer_threads:
        t.join()
        
    # Raise any captured errors
    if errors:
        raise StreamError(f"Parallel stream failed with {len(errors)} errors. First error: {errors[0]}")
