"""
Producer-Consumer Pattern Implementation

This module implements a thread-safe producer-consumer pattern using a bounded blocking queue.
"""

import threading
import queue
import time
from typing import Optional, Callable, Any
from pathlib import Path


class ThreadSafeQueue:
    """
    A thread-safe bounded blocking queue implementation.
    
    This class provides a thread-safe queue with a maximum size limit.
    When the queue is full, put operations will block until space is available.
    When the queue is empty, get operations will block until items are available.
    """
    
    def __init__(self, maxsize: int = 10):
        """
        Initialize the thread-safe queue.
        
        Args:
            maxsize: Maximum number of items the queue can hold (default: 10)
        """
        if maxsize <= 0:
            raise ValueError("maxsize must be greater than 0")
        self._queue = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._maxsize = maxsize
    
    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        """
        Put an item into the queue.
        
        Args:
            item: The item to add to the queue
            block: If True, block until a slot is available (default: True)
            timeout: Maximum time to wait if block is True (default: None)
        
        Raises:
            queue.Full: If block is False and queue is full
        """
        self._queue.put(item, block=block, timeout=timeout)
    
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Get an item from the queue.
        
        Args:
            block: If True, block until an item is available (default: True)
            timeout: Maximum time to wait if block is True (default: None)
        
        Returns:
            The item removed from the queue
        
        Raises:
            queue.Empty: If block is False and queue is empty
        """
        return self._queue.get(block=block, timeout=timeout)
    
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self._queue.qsize()
    
    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()
    
    def full(self) -> bool:
        """Return True if the queue is full, False otherwise."""
        return self._queue.full()
    
    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        self._queue.task_done()
    
    def join(self) -> None:
        """Block until all items in the queue have been processed."""
        self._queue.join()


class Producer:
    """
    A producer that reads data from a file and puts it into a queue.
    """
    
    def __init__(self, queue: ThreadSafeQueue, file_path: str, name: str = "Producer"):
        """
        Initialize the producer.
        
        Args:
            queue: The thread-safe queue to put items into
            file_path: Path to the file to read from
            name: Name identifier for this producer (default: "Producer")
        """
        self._queue = queue
        self._file_path = Path(file_path)
        self._name = name
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._items_produced = 0
        self._lock = threading.Lock()
    
    def _read_file(self) -> list:
        """
        Read all lines from the file.
        
        Returns:
            List of lines from the file
        
        Raises:
            FileNotFoundError: If the file doesn't exist
            IOError: If there's an error reading the file
        """
        if not self._file_path.exists():
            raise FileNotFoundError(f"File not found: {self._file_path}")
        
        with open(self._file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        return lines
    
    def _produce(self) -> None:
        """Main producer loop that reads from file and puts items into queue."""
        try:
            lines = self._read_file()
            for line in lines:
                if self._stop_event.is_set():
                    break
                
                # Put the line into the queue (will block if queue is full)
                self._queue.put(line)
                
                with self._lock:
                    self._items_produced += 1
                
                print(f"[{self._name}] Produced: {line}")
        except FileNotFoundError as e:
            print(f"[{self._name}] Error: {e}")
        except Exception as e:
            print(f"[{self._name}] Unexpected error: {e}")
        finally:
            # Signal that production is complete by putting None
            # This allows consumers to know when to stop
            try:
                self._queue.put(None, block=False)
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
    """
    A consumer that reads data from a queue and processes it.
    """
    
    def __init__(self, queue: ThreadSafeQueue, output_file: str, name: str = "Consumer"):
        """
        Initialize the consumer.
        
        Args:
            queue: The thread-safe queue to get items from
            output_file: Path to the file to write processed items to
            name: Name identifier for this consumer (default: "Consumer")
        """
        self._queue = queue
        self._output_file = Path(output_file)
        self._name = name
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._items_consumed = 0
        self._lock = threading.Lock()
        self._file_lock = threading.Lock()  # Lock for file writing
    
    def _process_item(self, item: str) -> str:
        """
        Process a single item (can be overridden for custom processing).
        
        Args:
            item: The item to process
        
        Returns:
            The processed item
        """
        # Default processing: convert to uppercase
        return item.upper()
    
    def _consume(self) -> None:
        """Main consumer loop that gets items from queue and processes them."""
        try:
            # Ensure output directory exists
            self._output_file.parent.mkdir(parents=True, exist_ok=True)
            
            while not self._stop_event.is_set():
                try:
                    # Get item from queue (will block if queue is empty)
                    item = self._queue.get(timeout=1.0)
                    
                    # None is a sentinel value indicating production is complete
                    if item is None:
                        # Mark the None as done before putting it back for other consumers
                        self._queue.task_done()
                        # Put None back for other consumers
                        try:
                            self._queue.put(None, block=False)
                        except queue.Full:
                            # If queue is full, that's okay - other consumers already have the sentinel
                            pass
                        break
                    
                    # Process the item
                    processed = self._process_item(item)
                    
                    # Write to file (thread-safe)
                    with self._file_lock:
                        with open(self._output_file, 'a', encoding='utf-8') as f:
                            f.write(processed + '\n')
                    
                    with self._lock:
                        self._items_consumed += 1
                    
                    print(f"[{self._name}] Consumed and processed: {item} -> {processed}")
                    
                    # Mark task as done
                    self._queue.task_done()
                    
                except queue.Empty:
                    # Timeout occurred, check if we should continue
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
    
    def join(self) -> None:
        """Wait for the consumer thread to complete."""
        if self._thread is not None:
            self._thread.join()
    
    def get_items_consumed(self) -> int:
        """Get the number of items consumed."""
        with self._lock:
            return self._items_consumed


class ProducerConsumerOrchestrator:
    """
    Orchestrator that manages producers and consumers.
    """
    
    def __init__(self, queue_size: int = 10):
        """
        Initialize the orchestrator.
        
        Args:
            queue_size: Maximum size of the shared queue (default: 10)
        """
        self._queue = ThreadSafeQueue(maxsize=queue_size)
        self._producers: list[Producer] = []
        self._consumers: list[Consumer] = []
        self._lock = threading.Lock()
    
    def add_producer(self, file_path: str, name: Optional[str] = None) -> Producer:
        """
        Add a producer to the orchestrator.
        
        Args:
            file_path: Path to the file for the producer to read from
            name: Optional name for the producer
        
        Returns:
            The created Producer instance
        """
        if name is None:
            name = f"Producer-{len(self._producers) + 1}"
        
        producer = Producer(self._queue, file_path, name)
        with self._lock:
            self._producers.append(producer)
        return producer
    
    def add_consumer(self, output_file: str, name: Optional[str] = None) -> Consumer:
        """
        Add a consumer to the orchestrator.
        
        Args:
            output_file: Path to the file for the consumer to write to
            name: Optional name for the consumer
        
        Returns:
            The created Consumer instance
        """
        if name is None:
            name = f"Consumer-{len(self._consumers) + 1}"
        
        consumer = Consumer(self._queue, output_file, name)
        with self._lock:
            self._consumers.append(consumer)
        return consumer
    
    def run(self) -> None:
        """
        Start all producers and consumers and wait for completion.
        """
        # Start all consumers first
        for consumer in self._consumers:
            consumer.start()
        
        # Start all producers
        for producer in self._producers:
            producer.start()
        
        # Wait for all producers to complete
        for producer in self._producers:
            producer.join()
        
        # Wait a bit for consumers to process remaining items
        time.sleep(0.5)
        
        # Wait for all consumers to complete
        for consumer in self._consumers:
            consumer.join()
        
        # Note: We don't call queue.join() here because we use None sentinels
        # to signal completion. The thread joins above ensure all processing is complete.
    
    def get_stats(self) -> dict:
        """
        Get statistics about production and consumption.
        
        Returns:
            Dictionary with production and consumption statistics
        """
        total_produced = sum(p.get_items_produced() for p in self._producers)
        total_consumed = sum(c.get_items_consumed() for c in self._consumers)
        
        return {
            'total_produced': total_produced,
            'total_consumed': total_consumed,
            'queue_size': self._queue.qsize(),
            'producers': len(self._producers),
            'consumers': len(self._consumers)
        }

