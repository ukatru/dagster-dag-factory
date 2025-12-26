"""
Universal Streaming Utility - Producer-Consumer Pattern

Provides a reusable threading utility for all V2 operators.
Follows the producer-consumer pattern:
- Main thread feeds items to queue via producer callback
- Worker threads process items via worker callback
- No separate scanner thread
- Self-feeding pattern for DB operators

This eliminates per-operator threading code and provides consistent behavior.
"""
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional


class ProcessorItem:
    """Item to be processed by worker threads"""
    def __init__(self, name: str, data: Any):
        self.name = name
        self.data = data


class Processor:
    """
    Processor for threaded execution using producer-consumer pattern.
    
    Main thread feeds items to queue, worker threads process them.
    """
    
    def __init__(
        self,
        name: str,
        action_callback: Callable[['Processor', ProcessorItem, int], Any],
        on_complete: Optional[Callable[['Processor'], None]] = None,
        thread_size: int = 1,
        logger = None
    ):
        """
        Initialize processor with worker threads.
        
        Args:
            name: Processor name for logging
            action_callback: Function to process each item
                Signature: (processor, item, index) -> result
            on_complete: Optional callback when all items processed
            thread_size: Number of worker threads
            logger: Optional logger for progress tracking
        """
        self.name = name
        self.action_callback = action_callback
        self.on_complete = on_complete
        self.logger = logger
        
        self.queue: queue.Queue = queue.Queue()
        self.results: List[Any] = []
        self.results_lock = threading.Lock()
        self.error: Optional[Exception] = None
        self.item_count = 0
        self.start_time = time.time()
        
        self.thread_size = thread_size
        self.threads: List[threading.Thread] = []
        self._workers_started = False
    
    def _start_workers(self):
        """Lazily start worker threads."""
        if self._workers_started or self.thread_size <= 0:
            return
        
        if self.logger:
            self.logger.info(f"Starting {self.thread_size} parallel worker(s) for {self.name}")
            
        for i in range(self.thread_size):
            thread = threading.Thread(
                target=self._worker,
                name=f"{self.name}-Worker-{i+1}",
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        self._workers_started = True

    def put(self, item: ProcessorItem):
        """Add item to processing queue"""
        self._start_workers() # Start workers on first put
        self.queue.put(item)
        self.item_count += 1
    
    def _worker(self):
        """Worker thread that processes items from queue"""
        index = 0
        while True:
            try:
                # Get item from queue (blocks until available)
                item = self.queue.get(timeout=1)
                
                if item is None:  # Poison pill - shutdown signal
                    self.queue.task_done()
                    break
                
                # Process item
                result = self.action_callback(self, item, index)
                
                # Store result
                if result is not None:
                    with self.results_lock:
                        self.results.append(result)
                
                self.queue.task_done()
                index += 1
                
            except queue.Empty:
                # No items available, check if we should shutdown
                if self.error:
                    break
                continue
            except Exception as e:
                self.error = e
                if self.logger:
                    self.logger.error(f"Worker error: {e}")
                self.queue.task_done()
                break
    
    def wait(self):
        """Wait for all items to be processed"""
        # Wait for queue to be empty
        self.queue.join()
        
        # Send poison pills to shutdown workers
        for _ in self.threads:
            self.queue.put(None)
        
        # Wait for all threads to finish
        for thread in self.threads:
            thread.join()
        
        # Call completion callback
        if self.on_complete:
            self.on_complete(self)
        
        # Raise error if any occurred
        if self.error:
            raise self.error
        
        return self.results


def execute_streaming(
    producer_callback: Callable[[Processor], None],
    worker_callback: Callable[[Processor, ProcessorItem, int], Any],
    num_workers: int = 5,
    logger = None
) -> Dict[str, Any]:
    """
    Universal streaming execution using producer-consumer pattern.
    
    This utility handles all threading logic so operators don't need to.
    
    Args:
        producer_callback: Function that feeds items to queue (runs in main thread)
            Signature: (processor) -> None
            Example: processor.put(ProcessorItem(name='file1', data=file_info))
        
        worker_callback: Function that processes each item (runs in worker threads)
            Signature: (processor, item, index) -> result
            Example: return transfer_file(item.data)
        
        num_workers: Number of worker threads (default: 5)
        logger: Optional logger for progress tracking
    
    Returns:
        Dict containing execution results and statistics
    """
    start_time = time.time()
    
    # Create processor with worker threads
    processor = Processor(
        name='streaming',
        action_callback=worker_callback,
        thread_size=num_workers,
        logger=logger
    )
    
    # Main thread feeds items to queue
    producer_callback(processor)
    
    # Track number of source items processed
    source_items_count = processor.item_count
    
    # Wait for all items to be processed
    results = processor.wait()
    
    duration = time.time() - start_time
    
    # Flatten results (workers may return lists from smart buffer)
    flattened_results = []
    for result in results:
        if isinstance(result, list):
            flattened_results.extend(result)
        else:
            flattened_results.append(result)
    
    # Generate statistics
    from dagster_dag_factory.factory.helpers.stats import generate_transfer_stats
    summary = generate_transfer_stats(flattened_results, duration)
    summary['mode'] = 'streaming'
    summary['source_items_count'] = source_items_count  # Track source items separately
    
    return {
        'summary': summary,
        'files': flattened_results
    }
