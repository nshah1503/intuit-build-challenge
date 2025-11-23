import csv
from pathlib import Path
from typing import List, Optional
from .models import SalesRecord


def read_sales_data(csv_path: str) -> List[SalesRecord]:
    path = Path(csv_path)
    
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    records = []
    
    # Try different encodings in case the file is not UTF-8
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']
    file_handle = None
    encoding_used = None
    
    for encoding in encodings:
        try:
            file_handle = open(path, 'r', encoding=encoding)
            # Try to read first line to verify encoding works
            file_handle.readline()
            file_handle.seek(0)  # Reset to beginning
            encoding_used = encoding
            break
        except (UnicodeDecodeError, UnicodeError):
            if file_handle:
                file_handle.close()
                file_handle = None
            continue
    
    if file_handle is None:
        raise ValueError(f"Unable to read file with any supported encoding: {csv_path}")
    
    try:
        reader = csv.reader(file_handle)
        
        # Read headers
        headers = next(reader)
        
        # Validate headers
        required_headers = [
            'Row ID', 'Order ID', 'Order Date', 'Ship Date', 'Ship Mode',
            'Customer ID', 'Customer Name', 'Segment', 'Country', 'City',
            'State', 'Postal Code', 'Region', 'Product ID', 'Category',
            'Sub-Category', 'Product Name', 'Sales', 'Quantity', 'Discount', 'Profit'
        ]
        
        if not all(header in headers for header in required_headers):
            raise ValueError("CSV file is missing required headers")
        
        # Parse each row
        for row_num, row in enumerate(reader, start=2):
            if not row or len(row) != len(headers):
                continue  # Skip empty or malformed rows
            
            try:
                record = SalesRecord.from_csv_row(row, headers)
                records.append(record)
            except (ValueError, KeyError) as e:
                # Log error but continue processing
                print(f"Warning: Skipping row {row_num} due to error: {e}")
                continue
    finally:
        file_handle.close()
    
    return records


def validate_record(record: SalesRecord) -> bool:
    """
    Validate a sales record.
    
    Args:
        record: SalesRecord to validate
        
    Returns:
        True if valid, False otherwise
    """
    return (
        record.sales >= 0 and
        record.quantity > 0 and
        0 <= record.discount <= 1 and
        record.order_date <= record.ship_date
    )

