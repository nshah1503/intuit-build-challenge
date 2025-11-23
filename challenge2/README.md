# Challenge 2: Sales Data Analysis

Functional programming implementation for analyzing sales data using Python's built-in functional features (lambda, map, filter, reduce, itertools.groupby).

## Setup

```bash
cd challenge2
pip install -r requirements.txt  # Optional, only for running tests
```

The `sales_data.csv` dataset is included in the directory (9,994 records).

## Running

```bash
python main.py                    # Uses sales_data.csv in current directory
python main.py /path/to/file.csv  # Custom CSV path
```

## Testing

```bash
pytest tests/ -v
```

## Features

- **Basic Aggregations**: Total/average sales, profit, quantity, discounts, min/max values
- **Grouping Analysis**: By region, category, customer segment, state
- **Multi-Level Grouping**: Region+Category, Category+Sub-Category, Segment+Region
- **Time-Based Analysis**: Sales by year, month, trends
- **Advanced Operations**: High discount orders, profit margins, top products, negative profit analysis

## Sample Output

```
======================================================================
=== Basic Aggregations ===
======================================================================

Total Sales:        $2,297,200.86
Total Profit:       $286,397.02
Average Sales:      $229.86
Average Profit:     $28.66
...

======================================================================
=== Analysis by Region ===
======================================================================

Total Sales by Region:
----------------------------------------------------------------------
  Central                       : $501,239.89
  East                          : $678,781.24
  South                         : $391,721.91
  West                          : $725,457.82
...
```

## Implementation Details

- Uses lambda expressions, map(), filter(), reduce() for all operations
- itertools.groupby() for grouping operations
- Dataclasses for data modeling
- No external dependencies (except pytest for testing)
- All results printed to console

## Project Structure

```
challenge2/
├── sales_data.csv          # Dataset (included)
├── main.py                 # Entry point
├── sales_analysis/         # Analysis package
│   ├── models.py           # Data models
│   ├── csv_reader.py       # CSV parsing
│   └── analyzer.py         # Analysis methods
└── tests/                  # Unit tests (33 tests)

## Dataset Source

Dataset: [Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) on Kaggle
