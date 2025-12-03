"""
Example usage of the Producer-Consumer pattern implementation.

This script demonstrates how to use the ProducerConsumerOrchestrator
to process data from input files using multiple producers and consumers.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from producer_consumer import ProducerConsumerOrchestrator


def create_sample_input_file(file_path: str, content: list[str]) -> None:
    """Create a sample input file with the given content."""
    with open(file_path, 'w', encoding='utf-8') as f:
        for line in content:
            f.write(line + '\n')


def main():
    """Entry Point"""
    example_dir = Path(__file__).parent
    example_dir.mkdir(exist_ok=True)
    
    input_file1 = example_dir / "input1.txt"
    input_file2 = example_dir / "input2.txt"
    output_file1 = example_dir / "output1.txt"
    output_file2 = example_dir / "output2.txt"
    
    create_sample_input_file(
        str(input_file1),
        ["carrot", "broccoli", "spinach", "tomato", "cucumber", "pepper", "lettuce"]
    )
    
    create_sample_input_file(
        str(input_file2),
        ["apple", "banana", "orange", "grape", "kiwi", "mango", "strawberry"]
    )
    
    print("=" * 60)
    print("Producer-Consumer Pattern Example")
    print("=" * 60)
    print(f"\nInput files created:")
    print(f"  - {input_file1}")
    print(f"  - {input_file2}")
    print(f"\nOutput files will be written to:")
    print(f"  - {output_file1}")
    print(f"  - {output_file2}")
    print("\n" + "=" * 60)
    print("Starting producer-consumer processing...")
    print("=" * 60 + "\n")
    
    orchestrator = ProducerConsumerOrchestrator(queue_size=5)
    
    orchestrator.add_producer(str(input_file1), "Producer-1", thread_name="VegetableThread")
    orchestrator.add_producer(str(input_file2), "Producer-2", thread_name="FruitThread")
    
    orchestrator.add_consumer(str(output_file1), "Consumer-1", thread_name="ProcessorThread-1")
    orchestrator.add_consumer(str(output_file2), "Consumer-2", thread_name="ProcessorThread-2")
    
    # Run the orchestrator
    orchestrator.run()
    
    # Get and display statistics
    stats = orchestrator.get_stats()
    print("\n" + "=" * 60)
    print("Processing Complete!")
    print("=" * 60)
    print(f"\nStatistics:")
    print(f"  Total items produced: {stats['total_produced']}")
    print(f"  Total items consumed: {stats['total_consumed']}")
    print(f"  Queue size: {stats['queue_size']}")
    print(f"  Number of producers: {stats['producers']}")
    print(f"  Number of consumers: {stats['consumers']}")
    
    # Display output file contents
    print("\n" + "=" * 60)
    print("Output File Contents:")
    print("=" * 60)
    
    if output_file1.exists():
        print(f"\n{output_file1}:")
        with open(output_file1, 'r') as f:
            for line in f:
                print(f"  {line.strip()}")
    
    if output_file2.exists():
        print(f"\n{output_file2}:")
        with open(output_file2, 'r') as f:
            for line in f:
                print(f"  {line.strip()}")
    
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

