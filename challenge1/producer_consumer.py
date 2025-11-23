"""
Producer-Consumer Pattern Implementation

Thread-safe producer-consumer pattern using threading.Thread and queue.Queue.
We use threading (not multiprocessing) because this is I/O-bound work (file reading/writing),
not CPU-bound computation. Threading is more efficient for I/O operations.
"""

import threading
import queue
import time
from typing import Optional, Any
from pathlib import Path


class ThreadSafeQueue:
    """Thread-safe bounded blocking queue."""
    
    def __init__(self, maxsize: int = 10):
        """Initialize queue with maximum size."""
        if maxsize <= 0:
            raise ValueError("maxsize must be greater than 0")
        self._queue = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._maxsize = maxsize
    
    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        """Put item into queue. Blocks if full (unless block=False).
        
        Thread synchronization: queue.Queue handles internal locking.
        When queue is full, this blocks until space is available (wait/notify mechanism).
        """
        self._queue.put(item, block=block, timeout=timeout)
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """Get item from queue. Blocks if empty (unless block=False).
        
        Thread synchronization: queue.Queue handles internal locking.
        When queue is empty, this blocks until item is available (wait/notify mechanism).
        """
        return self._queue.get(block=block, timeout=timeout)
    
    def qsize(self) -> int:
        """Return approximate queue size."""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty()
    
    def full(self) -> bool:
        """Check if queue is full."""
        return self._queue.full()
    
    def task_done(self) -> None:
        """Mark task as complete."""
        self._queue.task_done()
    
    def join(self) -> None:
        """Block until all tasks are done."""
        self._queue.join()


class Producer:
    """Producer reads from file and puts items into queue."""
    
    def __init__(self, queue: ThreadSafeQueue, file_path: str, name: str = "Producer"):
        """Initialize producer with queue, file path, and name."""
        self._queue = queue
        self._file_path = Path(file_path)
        self._name = name
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._items_produced = 0
        self._lock = threading.Lock()
    
    def _read_file(self) -> list:
        """Read all non-empty lines from file."""
        if not self._file_path.exists():
            raise FileNotFoundError(f"File not found: {self._file_path}")
        
        with open(self._file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    
    def _produce(self) -> None:
        """Producer loop: read file and put items into queue."""
        try:
            for line in self._read_file():
                if self._stop_event.is_set():
                    break
                self._queue.put(line)
                with self._lock:
                    self._items_produced += 1
                print(f"[{self._name}] Produced: {line}")
        except FileNotFoundError as e:
            print(f"[{self._name}] Error: {e}")
        except Exception as e:
            print(f"[{self._name}] Unexpected error: {e}")
        finally:
            try:
                self._queue.put(None, block=False)  # Sentinel to signal completion
            except queue.Full:
                pass
    
    def start(self) -> None:
        """Start the producer thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("Producer is already running")
        
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._produce, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the producer thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
    
    def join(self) -> None:
        """Wait for the producer thread to complete."""
        if self._thread is not None:
            self._thread.join()
    
    def get_items_produced(self) -> int:
        """Get the number of items produced."""
        with self._lock:
            return self._items_produced


class Consumer:
    """Consumer gets items from queue, processes them, and writes to file."""
    
    def __init__(self, queue: ThreadSafeQueue, output_file: str, name: str = "Consumer"):
        """Initialize consumer with queue, output file, and name."""
        self._queue = queue
        self._output_file = Path(output_file)
        self._name = name
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._items_consumed = 0
        self._lock = threading.Lock()
        self._file_lock = threading.Lock()
    
    def _process_item(self, item: str) -> str:
        """Process item (default: convert to uppercase)."""
        return item.upper()
    
    def _consume(self) -> None:
        """Consumer loop: get items from queue, process, and write to file."""
        try:
            self._output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Main consumer loop - continues until stop_event is set
            while not self._stop_event.is_set():
                try:
                    # Blocking get with timeout - waits up to 1 second for items
                    # This allows checking stop_event periodically
                    item = self._queue.get(timeout=1.0)
                    
                    # Sentinel value (None) signals all producers have finished
                    # Each producer puts one None when done, consumers pass it along
                    if item is None:
                        self._queue.task_done()
                        try:
                            # Put None back for other consumers to see
                            self._queue.put(None, block=False)
                        except queue.Full:
                            pass  # Other consumers already have the sentinel
                        break
                    
                    # Process item (default: convert to uppercase)
                    processed = self._process_item(item)
                    
                    # Thread-safe file writing using file lock
                    with self._file_lock:
                        with open(self._output_file, 'a', encoding='utf-8') as f:
                            f.write(processed + '\n')
                    
                    # Thread-safe counter update
                    with self._lock:
                        self._items_consumed += 1
                    
                    print(f"[{self._name}] Consumed and processed: {item} -> {processed}")
                    # Mark task as done for queue.join() tracking
                    self._queue.task_done()
                    
                except queue.Empty:
                    # Timeout occurred - continue loop to check stop_event
                    continue
        except Exception as e:
            print(f"[{self._name}] Unexpected error: {e}")
    
    def start(self) -> None:
        """Start the consumer thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("Consumer is already running")
        
        self._stop_event.clear()
        # Clear output file if it exists
        if self._output_file.exists():
            self._output_file.unlink()
        
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the consumer thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
    
    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for the consumer thread to complete."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)
    
    def get_items_consumed(self) -> int:
        """Get the number of items consumed."""
        with self._lock:
            return self._items_consumed


class ProducerConsumerOrchestrator:
    """Orchestrator manages multiple producers and consumers."""
    
    def __init__(self, queue_size: int = 10):
        """Initialize orchestrator with queue size."""
        self._queue = ThreadSafeQueue(maxsize=queue_size)
        self._producers: list[Producer] = []
        self._consumers: list[Consumer] = []
        self._lock = threading.Lock()
    
    def add_producer(self, file_path: str, name: Optional[str] = None) -> Producer:
        """Add producer. Returns Producer instance."""
        if name is None:
            name = f"Producer-{len(self._producers) + 1}"
        producer = Producer(self._queue, file_path, name)
        with self._lock:
            self._producers.append(producer)
        return producer
    
    def add_consumer(self, output_file: str, name: Optional[str] = None) -> Consumer:
        """Add consumer. Returns Consumer instance."""
        if name is None:
            name = f"Consumer-{len(self._consumers) + 1}"
        consumer = Consumer(self._queue, output_file, name)
        with self._lock:
            self._consumers.append(consumer)
        return consumer
    
    def run(self) -> None:
        """Start all producers/consumers and wait for completion."""
        # Start consumers first so they're ready when producers start producing
        for consumer in self._consumers:
            consumer.start()
        # Start all producers
        for producer in self._producers:
            producer.start()
        # Wait for all producers to complete (they'll put None sentinels when done)
        for producer in self._producers:
            producer.join()
        # Brief wait for consumers to process remaining items in queue
        time.sleep(0.5)
        # Signal all consumers to stop (handles case where consumer times out before seeing None)
        # This ensures clean shutdown even if sentinel mechanism has issues
        for consumer in self._consumers:
            consumer._stop_event.set()
        # Wait for all consumer threads to finish
        for consumer in self._consumers:
            consumer.join()
    
    def get_stats(self) -> dict:
        """Return statistics dictionary."""
        return {
            'total_produced': sum(p.get_items_produced() for p in self._producers),
            'total_consumed': sum(c.get_items_consumed() for c in self._consumers),
            'queue_size': self._queue.qsize(),
            'producers': len(self._producers),
            'consumers': len(self._consumers)
        }

