# intuit-build-challenge
 Intuit's build challenge assignment solutions.

## Challenge 1: Producer-Consumer Pattern Implementation

This challenge implements a thread-safe producer-consumer pattern using a bounded blocking queue in Python.

### Features

- **ThreadSafeQueue**: A thread-safe bounded blocking queue implementation
- **Producer**: Reads data from files and puts items into the queue
- **Consumer**: Gets items from the queue, processes them, and writes to output files
- **ProducerConsumerOrchestrator**: Manages multiple producers and consumers

### Installation

1. Navigate to the challenge1 directory:
   ```bash
   cd challenge1
   ```

2. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```

3. Activate the virtual environment:
   ```bash
   source venv/bin/activate  # On macOS/Linux
   # or
   venv\Scripts\activate  # On Windows
   ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage

#### Basic Usage

```python
from producer_consumer import ProducerConsumerOrchestrator

# Create orchestrator with queue size of 10
orchestrator = ProducerConsumerOrchestrator(queue_size=10)

# Add producers (read from input files)
orchestrator.add_producer("input1.txt", "Producer-1")
orchestrator.add_producer("input2.txt", "Producer-2")

# Add consumers (write to output files)
orchestrator.add_consumer("output1.txt", "Consumer-1")
orchestrator.add_consumer("output2.txt", "Consumer-2")

# Run the orchestrator
orchestrator.run()

# Get statistics
stats = orchestrator.get_stats()
print(f"Produced: {stats['total_produced']}, Consumed: {stats['total_consumed']}")
```

#### Running the Example

```bash
cd challenge1
source venv/bin/activate
python examples/example_usage.py
```

### Running Tests

Run all tests:
```bash
cd challenge1
source venv/bin/activate
pytest tests/ -v
```

Run tests with coverage:
```bash
pytest tests/ --cov=producer_consumer --cov-report=html
```

### Project Structure

```
challenge1/
├── producer_consumer.py    # Main implementation
├── requirements.txt         # Dependencies
├── tests/
│   ├── __init__.py
│   └── test_producer_consumer.py  # Unit tests
└── examples/
    └── example_usage.py    # Example usage script
```

### Implementation Details

- **Thread Safety**: Uses Python's `queue.Queue` for thread-safe operations
- **Bounded Queue**: Queue has a maximum size to prevent memory issues
- **Blocking Operations**: Producers and consumers block appropriately when queue is full/empty
- **File I/O**: Thread-safe file reading and writing operations
- **Graceful Shutdown**: Uses sentinel values (None) to signal completion

### Testing

The test suite includes:
- ThreadSafeQueue: Basic operations, thread safety, blocking behavior
- Producer: File reading, queue operations, thread safety
- Consumer: Queue operations, file writing, thread safety
- ProducerConsumerOrchestrator: End-to-end scenarios with multiple producers/consumers
