"""
Unit tests for CSV Reader module.
"""

import unittest
import tempfile
import os
from pathlib import Path
from sales_analysis.csv_reader import read_sales_data, validate_record
from sales_analysis.models import SalesRecord
from datetime import datetime


class TestCSVReader(unittest.TestCase):
    """Test cases for CSV reader functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a sample CSV file for testing
        self.sample_csv_content = """Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit
1,CA-1000-100000,2022-01-15,2022-01-20,Standard Class,CG-1000,Customer 1,Consumer,United States,Los Angeles,California,90001,West,TECPHO-10000,Technology,Phones,Test Phone,1000.0,2,0.1,200.0
2,CA-2000-200000,2022-02-15,2022-02-18,First Class,CG-2000,Customer 2,Corporate,United States,New York,New York,10001,East,OFFAPP-20000,Office Supplies,Appliances,Test Appliance,2000.0,3,0.15,300.0
3,CA-3000-300000,2022-03-15,2022-03-17,Same Day,CG-3000,Customer 3,Home Office,United States,Chicago,Illinois,60601,Central,FURCHA-30000,Furniture,Chairs,Test Chair,1500.0,1,0.05,150.0"""
        
        # Create temporary CSV file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_file.write(self.sample_csv_content)
        self.temp_file.close()
        self.temp_file_path = self.temp_file.name
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_file_path):
            os.unlink(self.temp_file_path)
    
    def test_read_valid_csv(self):
        """Test reading a valid CSV file."""
        records = read_sales_data(self.temp_file_path)
        
        self.assertEqual(len(records), 3)
        self.assertIsInstance(records[0], SalesRecord)
        self.assertEqual(records[0].row_id, 1)
        self.assertEqual(records[0].order_id, "CA-1000-100000")
        self.assertEqual(records[0].sales, 1000.0)
        self.assertEqual(records[0].profit, 200.0)
    
    def test_read_csv_file_not_found(self):
        """Test reading a non-existent CSV file."""
        with self.assertRaises(FileNotFoundError):
            read_sales_data("nonexistent_file.csv")
    
    def test_csv_date_parsing(self):
        """Test that dates are parsed correctly."""
        records = read_sales_data(self.temp_file_path)
        
        self.assertIsInstance(records[0].order_date, datetime)
        self.assertEqual(records[0].order_date.year, 2022)
        self.assertEqual(records[0].order_date.month, 1)
        self.assertEqual(records[0].order_date.day, 15)
    
    def test_csv_numeric_fields(self):
        """Test that numeric fields are parsed correctly."""
        records = read_sales_data(self.temp_file_path)
        
        self.assertEqual(records[0].sales, 1000.0)
        self.assertEqual(records[0].quantity, 2)
        self.assertEqual(records[0].discount, 0.1)
        self.assertEqual(records[0].profit, 200.0)
        self.assertEqual(records[0].postal_code, 90001)
    
    def test_validate_record_valid(self):
        """Test validation of a valid record."""
        record = SalesRecord(
            row_id=1, order_id="CA-1000", order_date=datetime(2022, 1, 1),
            ship_date=datetime(2022, 1, 5), ship_mode="Standard",
            customer_id="CG-1000", customer_name="Test", segment="Consumer",
            country="US", city="LA", state="CA", postal_code=90001,
            region="West", product_id="PROD-1", category="Tech",
            sub_category="Phones", product_name="Phone", sales=1000.0,
            quantity=2, discount=0.1, profit=200.0
        )
        
        self.assertTrue(validate_record(record))
    
    def test_validate_record_invalid_sales(self):
        """Test validation fails for negative sales."""
        record = SalesRecord(
            row_id=1, order_id="CA-1000", order_date=datetime(2022, 1, 1),
            ship_date=datetime(2022, 1, 5), ship_mode="Standard",
            customer_id="CG-1000", customer_name="Test", segment="Consumer",
            country="US", city="LA", state="CA", postal_code=90001,
            region="West", product_id="PROD-1", category="Tech",
            sub_category="Phones", product_name="Phone", sales=-100.0,
            quantity=2, discount=0.1, profit=200.0
        )
        
        self.assertFalse(validate_record(record))
    
    def test_validate_record_invalid_dates(self):
        """Test validation fails when ship date is before order date."""
        record = SalesRecord(
            row_id=1, order_id="CA-1000", order_date=datetime(2022, 1, 10),
            ship_date=datetime(2022, 1, 5), ship_mode="Standard",
            customer_id="CG-1000", customer_name="Test", segment="Consumer",
            country="US", city="LA", state="CA", postal_code=90001,
            region="West", product_id="PROD-1", category="Tech",
            sub_category="Phones", product_name="Phone", sales=1000.0,
            quantity=2, discount=0.1, profit=200.0
        )
        
        self.assertFalse(validate_record(record))


if __name__ == '__main__':
    unittest.main()

