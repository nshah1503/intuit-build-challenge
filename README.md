# Intuit Build Challenge

This repository contains solutions for Intuit's build challenge assignment. Each challenge demonstrates different software engineering concepts and best practices.

## Repository Structure

```
intuit-build-challenge/
├── challenge1/          # Producer-Consumer Pattern Implementation
├── challenge2/          # Sales Data Analysis with Functional Programming
└── README.md          
```

## Challenges

### Challenge 1: Producer-Consumer Pattern

A thread-safe producer-consumer pattern implementation using a bounded blocking queue in Python. This challenge demonstrates:

- Multi-threading and thread synchronization
- Bounded blocking queue implementation
- File I/O operations in a concurrent environment
- Comprehensive unit testing

**Quick Start:**
```bash
cd challenge1
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

For detailed information, see [Challenge 1 README](challenge1/README.md).

### Challenge 2: Sales Data Analysis

A functional programming implementation for analyzing sales data using Python's built-in functional features. This challenge demonstrates:

- Functional programming with lambda expressions, map, filter, reduce
- Data aggregation and grouping operations
- Stream-like operations using Python's functional tools
- Comprehensive unit testing

**Quick Start:**
```bash
cd challenge2
pip install -r requirements.txt
python main.py
pytest tests/ -v
```

For detailed information, see [Challenge 2 README](challenge2/README.md).

## Getting Started

Each challenge is self-contained with its own:
- Implementation code
- Unit tests
- Example usage scripts
- Documentation (README.md)
- Requirements file

Navigate to the specific challenge directory for installation and usage instructions.

## Requirements

- Python 3.7+
- pip
- Virtual environment (recommended)

## License

See [LICENSE](LICENSE) file for details.

## Development Note

Unit tests, boilerplate code, and general project structure were generated using Cursor AI agent to accelerate development while maintaining code quality and best practices.
