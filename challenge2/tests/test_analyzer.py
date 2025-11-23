"""
Unit tests for Sales Analyzer module.
Tests all aggregation and grouping operations.
"""

import unittest
from datetime import datetime
from sales_analysis.models import SalesRecord
from sales_analysis.analyzer import SalesAnalyzer


class TestSalesAnalyzer(unittest.TestCase):
    """Test cases for Sales Analyzer functionality."""
    
    def setUp(self):
        """Set up test fixtures with sample data."""
        self.records = [
            SalesRecord(
                row_id=1, order_id="CA-1000", order_date=datetime(2022, 1, 15),
                ship_date=datetime(2022, 1, 20), ship_mode="Standard Class",
                customer_id="CG-1000", customer_name="Customer 1", segment="Consumer",
                country="United States", city="Los Angeles", state="California",
                postal_code=90001, region="West", product_id="TECPHO-10000",
                category="Technology", sub_category="Phones", product_name="Phone A",
                sales=1000.0, quantity=2, discount=0.1, profit=200.0
            ),
            SalesRecord(
                row_id=2, order_id="CA-2000", order_date=datetime(2022, 2, 15),
                ship_date=datetime(2022, 2, 18), ship_mode="First Class",
                customer_id="CG-2000", customer_name="Customer 2", segment="Corporate",
                country="United States", city="New York", state="New York",
                postal_code=10001, region="East", product_id="OFFAPP-20000",
                category="Office Supplies", sub_category="Appliances", product_name="Appliance B",
                sales=2000.0, quantity=3, discount=0.15, profit=300.0
            ),
            SalesRecord(
                row_id=3, order_id="CA-3000", order_date=datetime(2022, 3, 15),
                ship_date=datetime(2022, 3, 17), ship_mode="Same Day",
                customer_id="CG-3000", customer_name="Customer 3", segment="Home Office",
                country="United States", city="Chicago", state="Illinois",
                postal_code=60601, region="Central", product_id="FURCHA-30000",
                category="Furniture", sub_category="Chairs", product_name="Chair C",
                sales=1500.0, quantity=1, discount=0.05, profit=150.0
            ),
            SalesRecord(
                row_id=4, order_id="CA-4000", order_date=datetime(2022, 1, 20),
                ship_date=datetime(2022, 1, 25), ship_mode="Standard Class",
                customer_id="CG-1000", customer_name="Customer 1", segment="Consumer",
                country="United States", city="Los Angeles", state="California",
                postal_code=90001, region="West", product_id="TECPHO-40000",
                category="Technology", sub_category="Phones", product_name="Phone A",
                sales=500.0, quantity=1, discount=0.0, profit=100.0
            ),
        ]
        
        self.analyzer = SalesAnalyzer(self.records)
    
    # ==================== BASIC AGGREGATIONS ====================
    
    def test_total_sales(self):
        """Test total sales calculation."""
        expected = 1000.0 + 2000.0 + 1500.0 + 500.0
        self.assertEqual(self.analyzer.total_sales(), expected)
    
    def test_total_profit(self):
        """Test total profit calculation."""
        expected = 200.0 + 300.0 + 150.0 + 100.0
        self.assertEqual(self.analyzer.total_profit(), expected)
    
    def test_average_sales(self):
        """Test average sales calculation."""
        expected = (1000.0 + 2000.0 + 1500.0 + 500.0) / 4
        self.assertEqual(self.analyzer.average_sales(), expected)
    
    def test_total_quantity(self):
        """Test total quantity calculation."""
        expected = 2 + 3 + 1 + 1
        self.assertEqual(self.analyzer.total_quantity(), expected)
    
    def test_average_discount(self):
        """Test average discount calculation."""
        expected = (0.1 + 0.15 + 0.05 + 0.0) / 4
        self.assertAlmostEqual(self.analyzer.average_discount(), expected, places=2)
    
    def test_max_sales(self):
        """Test maximum sales."""
        self.assertEqual(self.analyzer.max_sales(), 2000.0)
    
    def test_min_profit(self):
        """Test minimum profit."""
        self.assertEqual(self.analyzer.min_profit(), 100.0)
    
    # ==================== GROUPING BY REGION ====================
    
    def test_sales_by_region(self):
        """Test sales grouped by region."""
        result = self.analyzer.sales_by_region()
        self.assertEqual(result["West"], 1000.0 + 500.0)
        self.assertEqual(result["East"], 2000.0)
        self.assertEqual(result["Central"], 1500.0)
    
    def test_profit_by_region(self):
        """Test profit grouped by region."""
        result = self.analyzer.profit_by_region()
        self.assertEqual(result["West"], 200.0 + 100.0)
        self.assertEqual(result["East"], 300.0)
        self.assertEqual(result["Central"], 150.0)
    
    def test_top_region_by_sales(self):
        """Test top region by sales."""
        region, sales = self.analyzer.top_region_by_sales()
        self.assertEqual(region, "East")
        self.assertEqual(sales, 2000.0)
    
    # ==================== GROUPING BY CATEGORY ====================
    
    def test_sales_by_category(self):
        """Test sales grouped by category."""
        result = self.analyzer.sales_by_category()
        self.assertEqual(result["Technology"], 1000.0 + 500.0)
        self.assertEqual(result["Office Supplies"], 2000.0)
        self.assertEqual(result["Furniture"], 1500.0)
    
    def test_profit_by_category(self):
        """Test profit grouped by category."""
        result = self.analyzer.profit_by_category()
        self.assertEqual(result["Technology"], 200.0 + 100.0)
        self.assertEqual(result["Office Supplies"], 300.0)
        self.assertEqual(result["Furniture"], 150.0)
    
    def test_top_category_by_profit(self):
        """Test top category by profit."""
        category, profit = self.analyzer.top_category_by_profit()
        # Technology and Office Supplies both have 300.0 profit
        # The result depends on which comes first in sorted order
        self.assertIn(category, ["Technology", "Office Supplies"])
        self.assertEqual(profit, 300.0)
    
    # ==================== GROUPING BY SEGMENT ====================
    
    def test_sales_by_segment(self):
        """Test sales grouped by segment."""
        result = self.analyzer.sales_by_segment()
        self.assertEqual(result["Consumer"], 1000.0 + 500.0)
        self.assertEqual(result["Corporate"], 2000.0)
        self.assertEqual(result["Home Office"], 1500.0)
    
    def test_profit_by_segment(self):
        """Test profit grouped by segment."""
        result = self.analyzer.profit_by_segment()
        self.assertEqual(result["Consumer"], 200.0 + 100.0)
        self.assertEqual(result["Corporate"], 300.0)
        self.assertEqual(result["Home Office"], 150.0)
    
    # ==================== GROUPING BY STATE ====================
    
    def test_sales_by_state(self):
        """Test sales grouped by state."""
        result = self.analyzer.sales_by_state()
        self.assertEqual(result["California"], 1000.0 + 500.0)
        self.assertEqual(result["New York"], 2000.0)
        self.assertEqual(result["Illinois"], 1500.0)
    
    def test_top_states_by_sales(self):
        """Test top states by sales."""
        result = self.analyzer.top_states_by_sales(2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], "New York")
        self.assertEqual(result[0][1], 2000.0)
    
    # ==================== MULTI-LEVEL GROUPING ====================
    
    def test_sales_by_region_and_category(self):
        """Test sales grouped by region and category."""
        result = self.analyzer.sales_by_region_and_category()
        self.assertIn("West", result)
        self.assertIn("Technology", result["West"])
        self.assertEqual(result["West"]["Technology"], 1000.0 + 500.0)
    
    def test_profit_by_category_and_subcategory(self):
        """Test profit grouped by category and sub-category."""
        result = self.analyzer.profit_by_category_and_subcategory()
        self.assertIn("Technology", result)
        self.assertIn("Phones", result["Technology"])
        self.assertEqual(result["Technology"]["Phones"], 200.0 + 100.0)
    
    # ==================== TIME-BASED ANALYSIS ====================
    
    def test_sales_by_year(self):
        """Test sales grouped by year."""
        result = self.analyzer.sales_by_year()
        self.assertEqual(result[2022], 1000.0 + 2000.0 + 1500.0 + 500.0)
    
    def test_sales_by_month(self):
        """Test sales grouped by month."""
        result = self.analyzer.sales_by_month()
        # January has 2 orders (1000 + 500)
        self.assertEqual(result[1], 1000.0 + 500.0)
        # February has 1 order (2000)
        self.assertEqual(result[2], 2000.0)
        # March has 1 order (1500)
        self.assertEqual(result[3], 1500.0)
    
    # ==================== ADVANCED OPERATIONS ====================
    
    def test_orders_with_high_discount(self):
        """Test finding orders with high discount."""
        result = self.analyzer.orders_with_high_discount(0.1)
        # Should find orders with discount > 0.1 (0.15 and 0.1 itself is not > 0.1)
        self.assertGreaterEqual(len(result), 1)
    
    def test_count_high_discount_orders(self):
        """Test counting orders with high discount."""
        count = self.analyzer.count_high_discount_orders(0.05)
        # Orders with discount > 0.05: 0.1 and 0.15 (2 orders)
        self.assertEqual(count, 2)
    
    def test_top_products_by_sales(self):
        """Test top products by sales."""
        result = self.analyzer.top_products_by_sales(2)
        self.assertEqual(len(result), 2)
        # Appliance B has highest sales (2000.0)
        self.assertEqual(result[0][0], "Appliance B")
        self.assertEqual(result[0][1], 2000.0)
    
    def test_products_with_negative_profit(self):
        """Test finding products with negative profit."""
        # Add a record with negative profit
        negative_profit_record = SalesRecord(
            row_id=5, order_id="CA-5000", order_date=datetime(2022, 4, 1),
            ship_date=datetime(2022, 4, 5), ship_mode="Standard",
            customer_id="CG-5000", customer_name="Customer 5", segment="Consumer",
            country="United States", city="Miami", state="Florida",
            postal_code=33101, region="South", product_id="PROD-50000",
            category="Technology", sub_category="Phones", product_name="Phone D",
            sales=1000.0, quantity=1, discount=0.5, profit=-100.0
        )
        
        records_with_negative = self.records + [negative_profit_record]
        analyzer_with_negative = SalesAnalyzer(records_with_negative)
        
        result = analyzer_with_negative.products_with_negative_profit()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].profit, -100.0)
    
    def test_empty_records(self):
        """Test analyzer with empty records."""
        empty_analyzer = SalesAnalyzer([])
        self.assertEqual(empty_analyzer.total_sales(), 0.0)
        self.assertEqual(empty_analyzer.average_sales(), 0.0)
        self.assertEqual(len(empty_analyzer.sales_by_region()), 0)


if __name__ == '__main__':
    unittest.main()

