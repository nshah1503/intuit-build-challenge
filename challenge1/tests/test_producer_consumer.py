"""
Unit tests for the producer-consumer pattern implementation.
"""

import pytest
import threading
import time
import tempfile
import os
from pathlib import Path
from producer_consumer import (
    ThreadSafeQueue,
    Producer,
    Consumer,
    ProducerConsumerOrchestrator
)


class TestThreadSafeQueue:
    """Test cases for ThreadSafeQueue."""
    
    def test_queue_initialization(self):
        """Test queue initialization with valid maxsize."""
        queue = ThreadSafeQueue(maxsize=5)
        assert queue.qsize() == 0
        assert queue.empty() is True
        assert queue.full() is False
    
    def test_queue_initialization_invalid_maxsize(self):
        """Test queue initialization with invalid maxsize."""
        with pytest.raises(ValueError, match="maxsize must be greater than 0"):
            ThreadSafeQueue(maxsize=0)
        
        with pytest.raises(ValueError, match="maxsize must be greater than 0"):
            ThreadSafeQueue(maxsize=-1)
    
    def test_put_and_get(self):
        """Test basic put and get operations."""
        queue = ThreadSafeQueue(maxsize=5)
        queue.put("item1")
        queue.put("item2")
        
        assert queue.qsize() == 2
        assert queue.empty() is False
        
        item1 = queue.get()
        item2 = queue.get()
        
        assert item1 == "item1"
        assert item2 == "item2"
        assert queue.empty() is True
    
    def test_queue_full(self):
        """Test queue full condition."""
        queue = ThreadSafeQueue(maxsize=2)
        queue.put("item1")
        queue.put("item2")
        
        assert queue.full() is True
        assert queue.qsize() == 2
    
    def test_queue_blocking_put(self):
        """Test that put blocks when queue is full."""
        queue = ThreadSafeQueue(maxsize=1)
        queue.put("item1")
        
        # This should block, so we'll use a timeout
        start_time = time.time()
        result = []
        
        def put_item():
            queue.put("item2")
            result.append("put_complete")
        
        thread = threading.Thread(target=put_item)
        thread.start()
        
        # Wait a bit to ensure it's blocking
        time.sleep(0.1)
        assert len(result) == 0  # Should still be blocking
        
        # Get an item to unblock
        queue.get()
        thread.join(timeout=1.0)
        
        assert len(result) == 1
        assert queue.qsize() == 1
    
    def test_queue_blocking_get(self):
        """Test that get blocks when queue is empty."""
        queue = ThreadSafeQueue(maxsize=5)
        result = []
        
        def get_item():
            item = queue.get()
            result.append(item)
        
        thread = threading.Thread(target=get_item)
        thread.start()
        
        # Wait a bit to ensure it's blocking
        time.sleep(0.1)
        assert len(result) == 0  # Should still be blocking
        
        # Put an item to unblock
        queue.put("item1")
        thread.join(timeout=1.0)
        
        assert len(result) == 1
        assert result[0] == "item1"
    
    def test_queue_non_blocking_put(self):
        """Test non-blocking put raises Full exception."""
        queue = ThreadSafeQueue(maxsize=1)
        queue.put("item1")
        
        with pytest.raises(Exception):  # queue.Full
            queue.put("item2", block=False)
    
    def test_queue_non_blocking_get(self):
        """Test non-blocking get raises Empty exception."""
        queue = ThreadSafeQueue(maxsize=5)
        
        with pytest.raises(Exception):  # queue.Empty
            queue.get(block=False)
    
    def test_task_done_and_join(self):
        """Test task_done and join functionality."""
        queue = ThreadSafeQueue(maxsize=5)
        queue.put("item1")
        queue.put("item2")
        
        item1 = queue.get()
        queue.task_done()
        
        item2 = queue.get()
        queue.task_done()
        
        # Join should complete immediately since all tasks are done
        queue.join()
        
        assert item1 == "item1"
        assert item2 == "item2"


class TestProducer:
    """Test cases for Producer."""
    
    def test_producer_initialization(self):
        """Test producer initialization."""
        queue = ThreadSafeQueue(maxsize=5)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\nline3\n")
            temp_file = f.name
        
        try:
            producer = Producer(queue, temp_file, "TestProducer")
            assert producer._name == "TestProducer"
            assert producer._file_path == Path(temp_file)
            assert producer.get_items_produced() == 0
        finally:
            os.unlink(temp_file)
    
    def test_producer_read_file(self):
        """Test producer reads file correctly."""
        queue = ThreadSafeQueue(maxsize=10)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\nline3\n")
            temp_file = f.name
        
        try:
            producer = Producer(queue, temp_file)
            lines = producer._read_file()
            assert lines == ["line1", "line2", "line3"]
        finally:
            os.unlink(temp_file)
    
    def test_producer_read_nonexistent_file(self):
        """Test producer handles nonexistent file."""
        queue = ThreadSafeQueue(maxsize=10)
        producer = Producer(queue, "/nonexistent/file.txt")
        
        with pytest.raises(FileNotFoundError):
            producer._read_file()
    
    def test_producer_produces_items(self):
        """Test producer puts items into queue."""
        queue = ThreadSafeQueue(maxsize=10)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\nline3\n")
            temp_file = f.name
        
        try:
            producer = Producer(queue, temp_file)
            producer.start()
            producer.join()
            
            # Check that items were produced
            assert producer.get_items_produced() == 3
            
            # Check that items are in queue (plus None sentinel)
            items = []
            while not queue.empty():
                item = queue.get(block=False)
                if item is not None:
                    items.append(item)
            
            assert len(items) == 3
            assert "line1" in items
            assert "line2" in items
            assert "line3" in items
        finally:
            os.unlink(temp_file)
    
    def test_producer_stop(self):
        """Test producer stop functionality."""
        queue = ThreadSafeQueue(maxsize=10)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\nline3\n")
            temp_file = f.name
        
        try:
            producer = Producer(queue, temp_file)
            producer.start()
            time.sleep(0.1)  # Let it start
            producer.stop()
            
            # Producer should have stopped
            assert producer._stop_event.is_set()
        finally:
            os.unlink(temp_file)


class TestConsumer:
    """Test cases for Consumer."""
    
    def test_consumer_initialization(self):
        """Test consumer initialization."""
        queue = ThreadSafeQueue(maxsize=5)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            temp_output = f.name
        
        try:
            consumer = Consumer(queue, temp_output, "TestConsumer")
            assert consumer._name == "TestConsumer"
            assert consumer._output_file == Path(temp_output)
            assert consumer.get_items_consumed() == 0
        finally:
            if os.path.exists(temp_output):
                os.unlink(temp_output)
    
    def test_consumer_processes_items(self):
        """Test consumer processes items from queue."""
        queue = ThreadSafeQueue(maxsize=10)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            temp_output = f.name
        
        try:
            # Put items in queue
            queue.put("hello")
            queue.put("world")
            queue.put(None)  # Sentinel
            
            consumer = Consumer(queue, temp_output)
            consumer.start()
            consumer.join()
            
            # Check that items were consumed
            assert consumer.get_items_consumed() == 2
            
            # Check output file
            with open(temp_output, 'r') as f:
                lines = [line.strip() for line in f.readlines()]
            
            assert "HELLO" in lines
            assert "WORLD" in lines
        finally:
            if os.path.exists(temp_output):
                os.unlink(temp_output)
    
    def test_consumer_stop(self):
        """Test consumer stop functionality."""
        queue = ThreadSafeQueue(maxsize=10)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            temp_output = f.name
        
        try:
            consumer = Consumer(queue, temp_output)
            consumer.start()
            time.sleep(0.1)  # Let it start
            consumer.stop()
            
            # Consumer should have stopped
            assert consumer._stop_event.is_set()
        finally:
            if os.path.exists(temp_output):
                os.unlink(temp_output)


class TestProducerConsumerOrchestrator:
    """Test cases for ProducerConsumerOrchestrator."""
    
    def test_orchestrator_initialization(self):
        """Test orchestrator initialization."""
        orchestrator = ProducerConsumerOrchestrator(queue_size=10)
        assert len(orchestrator._producers) == 0
        assert len(orchestrator._consumers) == 0
        assert orchestrator._queue.qsize() == 0
    
    def test_add_producer(self):
        """Test adding producers to orchestrator."""
        orchestrator = ProducerConsumerOrchestrator()
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\n")
            temp_file = f.name
        
        try:
            producer = orchestrator.add_producer(temp_file, "TestProducer")
            assert len(orchestrator._producers) == 1
            assert producer._name == "TestProducer"
        finally:
            os.unlink(temp_file)
    
    def test_add_consumer(self):
        """Test adding consumers to orchestrator."""
        orchestrator = ProducerConsumerOrchestrator()
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            temp_output = f.name
        
        try:
            consumer = orchestrator.add_consumer(temp_output, "TestConsumer")
            assert len(orchestrator._consumers) == 1
            assert consumer._name == "TestConsumer"
        finally:
            if os.path.exists(temp_output):
                os.unlink(temp_output)
    
    def test_end_to_end_single_producer_consumer(self):
        """Test end-to-end scenario with one producer and one consumer."""
        orchestrator = ProducerConsumerOrchestrator(queue_size=10)
        
        # Create input file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("hello\nworld\ntest\n")
            input_file = f.name
        
        # Create output file path
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            output_file = f.name
        
        try:
            orchestrator.add_producer(input_file)
            orchestrator.add_consumer(output_file)
            orchestrator.run()
            
            # Check stats
            stats = orchestrator.get_stats()
            assert stats['total_produced'] == 3
            assert stats['total_consumed'] == 3
            assert stats['producers'] == 1
            assert stats['consumers'] == 1
            
            # Check output file
            with open(output_file, 'r') as f:
                lines = [line.strip() for line in f.readlines()]
            
            assert "HELLO" in lines
            assert "WORLD" in lines
            assert "TEST" in lines
        finally:
            os.unlink(input_file)
            if os.path.exists(output_file):
                os.unlink(output_file)
    
    def test_end_to_end_multiple_producers_consumers(self):
        """Test end-to-end scenario with multiple producers and consumers."""
        orchestrator = ProducerConsumerOrchestrator(queue_size=10)
        
        # Create input files
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("file1_line1\nfile1_line2\n")
            input_file1 = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("file2_line1\nfile2_line2\n")
            input_file2 = f.name
        
        # Create output file paths
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            output_file1 = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            output_file2 = f.name
        
        try:
            orchestrator.add_producer(input_file1)
            orchestrator.add_producer(input_file2)
            orchestrator.add_consumer(output_file1)
            orchestrator.add_consumer(output_file2)
            orchestrator.run()
            
            # Check stats
            stats = orchestrator.get_stats()
            assert stats['total_produced'] == 4
            assert stats['total_consumed'] == 4
            assert stats['producers'] == 2
            assert stats['consumers'] == 2
        finally:
            os.unlink(input_file1)
            os.unlink(input_file2)
            if os.path.exists(output_file1):
                os.unlink(output_file1)
            if os.path.exists(output_file2):
                os.unlink(output_file2)
    
    def test_get_stats(self):
        """Test getting statistics from orchestrator."""
        orchestrator = ProducerConsumerOrchestrator(queue_size=10)
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("line1\nline2\n")
            input_file = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            output_file = f.name
        
        try:
            orchestrator.add_producer(input_file)
            orchestrator.add_consumer(output_file)
            orchestrator.run()
            
            stats = orchestrator.get_stats()
            assert 'total_produced' in stats
            assert 'total_consumed' in stats
            assert 'queue_size' in stats
            assert 'producers' in stats
            assert 'consumers' in stats
        finally:
            os.unlink(input_file)
            if os.path.exists(output_file):
                os.unlink(output_file)

