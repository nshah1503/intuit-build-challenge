from functools import reduce
from itertools import groupby
from operator import itemgetter
from typing import List, Dict, Tuple, Any
from .models import SalesRecord


class SalesAnalyzer:
    
    def __init__(self, records: List[SalesRecord]):
        self.records = records
    
    # ==================== BASIC AGGREGATIONS ====================
    
    def total_sales(self) -> float:
        """Calculate total sales across all records.
        
        Functional programming: Uses reduce() with lambda for aggregation.
        Lambda accumulates sales values: (accumulator, record) -> accumulator + record.sales
        """
        return reduce(lambda acc, r: acc + r.sales, self.records, 0.0)
    
    def total_profit(self) -> float:
        """Calculate total profit across all records.
        
        Functional programming: Uses reduce() with lambda for aggregation.
        """
        return reduce(lambda acc, r: acc + r.profit, self.records, 0.0)
    
    def average_sales(self) -> float:
        """Calculate average sales per order."""
        if not self.records:
            return 0.0
        total = self.total_sales()
        return total / len(self.records)
    
    def average_profit(self) -> float:
        """Calculate average profit per order."""
        if not self.records:
            return 0.0
        total = self.total_profit()
        return total / len(self.records)
    
    def total_quantity(self) -> int:
        """Calculate total quantity sold."""
        return reduce(lambda acc, r: acc + r.quantity, self.records, 0)
    
    def average_discount(self) -> float:
        """Calculate average discount applied."""
        if not self.records:
            return 0.0
        total_discount = reduce(lambda acc, r: acc + r.discount, self.records, 0.0)
        return total_discount / len(self.records)
    
    def max_sales(self) -> float:
        """Find maximum sales in a single order.
        
        Functional programming: Uses map() to extract sales values, then max() for aggregation.
        """
        if not self.records:
            return 0.0
        return max(map(lambda r: r.sales, self.records))
    
    def min_profit(self) -> float:
        """Find minimum profit in a single order.
        
        Functional programming: Uses map() to extract profit values, then min() for aggregation.
        """
        if not self.records:
            return 0.0
        return min(map(lambda r: r.profit, self.records))
    
    # ==================== GROUPING BY REGION ====================
    
    def sales_by_region(self) -> Dict[str, float]:
        """Calculate total sales grouped by region.
        
        Functional programming pattern:
        1. Sort records (required for groupby to work correctly)
        2. Use itertools.groupby() to group by region
        3. Use reduce() with lambda to sum sales within each group
        """
        # Sort by region - required for groupby to work correctly
        sorted_records = sorted(self.records, key=lambda r: r.region)
        
        # Group by region and aggregate using reduce
        result = {}
        for region, group in groupby(sorted_records, key=lambda r: r.region):
            # Reduce: accumulate sales values using lambda expression
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[region] = sales_sum
        
        return result
    
    def profit_by_region(self) -> Dict[str, float]:
        """Calculate total profit grouped by region."""
        sorted_records = sorted(self.records, key=lambda r: r.region)
        
        result = {}
        for region, group in groupby(sorted_records, key=lambda r: r.region):
            profit_sum = reduce(lambda acc, r: acc + r.profit, group, 0.0)
            result[region] = profit_sum
        
        return result
    
    def average_sales_by_region(self) -> Dict[str, float]:
        """Calculate average sales grouped by region."""
        sorted_records = sorted(self.records, key=lambda r: r.region)
        
        result = {}
        for region, group in groupby(sorted_records, key=lambda r: r.region):
            group_list = list(group)
            if group_list:
                total = reduce(lambda acc, r: acc + r.sales, group_list, 0.0)
                result[region] = total / len(group_list)
        
        return result
    
    def order_count_by_region(self) -> Dict[str, int]:
        """Count orders grouped by region."""
        sorted_records = sorted(self.records, key=lambda r: r.region)
        
        result = {}
        for region, group in groupby(sorted_records, key=lambda r: r.region):
            result[region] = len(list(group))
        
        return result
    
    def top_region_by_sales(self) -> Tuple[str, float]:
        """Find the top region by total sales."""
        sales_by_region = self.sales_by_region()
        if not sales_by_region:
            return ("", 0.0)
        return max(sales_by_region.items(), key=lambda x: x[1])
    
    # ==================== GROUPING BY CATEGORY ====================
    
    def sales_by_category(self) -> Dict[str, float]:
        """Calculate total sales grouped by product category."""
        sorted_records = sorted(self.records, key=lambda r: r.category)
        
        result = {}
        for category, group in groupby(sorted_records, key=lambda r: r.category):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[category] = sales_sum
        
        return result
    
    def profit_by_category(self) -> Dict[str, float]:
        """Calculate total profit grouped by product category."""
        sorted_records = sorted(self.records, key=lambda r: r.category)
        
        result = {}
        for category, group in groupby(sorted_records, key=lambda r: r.category):
            profit_sum = reduce(lambda acc, r: acc + r.profit, group, 0.0)
            result[category] = profit_sum
        
        return result
    
    def average_sales_by_category(self) -> Dict[str, float]:
        """Calculate average sales grouped by category."""
        sorted_records = sorted(self.records, key=lambda r: r.category)
        
        result = {}
        for category, group in groupby(sorted_records, key=lambda r: r.category):
            group_list = list(group)
            if group_list:
                total = reduce(lambda acc, r: acc + r.sales, group_list, 0.0)
                result[category] = total / len(group_list)
        
        return result
    
    def product_count_by_category(self) -> Dict[str, int]:
        """Count products grouped by category."""
        sorted_records = sorted(self.records, key=lambda r: r.category)
        
        result = {}
        for category, group in groupby(sorted_records, key=lambda r: r.category):
            result[category] = len(list(group))
        
        return result
    
    def top_category_by_profit(self) -> Tuple[str, float]:
        """Find the top category by total profit."""
        profit_by_category = self.profit_by_category()
        if not profit_by_category:
            return ("", 0.0)
        return max(profit_by_category.items(), key=lambda x: x[1])
    
    # ==================== GROUPING BY SEGMENT ====================
    
    def sales_by_segment(self) -> Dict[str, float]:
        """Calculate total sales grouped by customer segment."""
        sorted_records = sorted(self.records, key=lambda r: r.segment)
        
        result = {}
        for segment, group in groupby(sorted_records, key=lambda r: r.segment):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[segment] = sales_sum
        
        return result
    
    def profit_by_segment(self) -> Dict[str, float]:
        """Calculate total profit grouped by customer segment."""
        sorted_records = sorted(self.records, key=lambda r: r.segment)
        
        result = {}
        for segment, group in groupby(sorted_records, key=lambda r: r.segment):
            profit_sum = reduce(lambda acc, r: acc + r.profit, group, 0.0)
            result[segment] = profit_sum
        
        return result
    
    def average_sales_by_segment(self) -> Dict[str, float]:
        """Calculate average sales grouped by segment."""
        sorted_records = sorted(self.records, key=lambda r: r.segment)
        
        result = {}
        for segment, group in groupby(sorted_records, key=lambda r: r.segment):
            group_list = list(group)
            if group_list:
                total = reduce(lambda acc, r: acc + r.sales, group_list, 0.0)
                result[segment] = total / len(group_list)
        
        return result
    
    def customer_count_by_segment(self) -> Dict[str, int]:
        """Count unique customers grouped by segment."""
        sorted_records = sorted(self.records, key=lambda r: (r.segment, r.customer_id))
        
        result = {}
        for segment, group in groupby(sorted_records, key=lambda r: r.segment):
            unique_customers = set(map(lambda r: r.customer_id, group))
            result[segment] = len(unique_customers)
        
        return result
    
    def most_profitable_segment(self) -> Tuple[str, float]:
        """Find the most profitable customer segment."""
        profit_by_segment = self.profit_by_segment()
        if not profit_by_segment:
            return ("", 0.0)
        return max(profit_by_segment.items(), key=lambda x: x[1])
    
    # ==================== GROUPING BY STATE ====================
    
    def sales_by_state(self) -> Dict[str, float]:
        """Calculate total sales grouped by state."""
        sorted_records = sorted(self.records, key=lambda r: r.state)
        
        result = {}
        for state, group in groupby(sorted_records, key=lambda r: r.state):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[state] = sales_sum
        
        return result
    
    def profit_by_state(self) -> Dict[str, float]:
        """Calculate total profit grouped by state."""
        sorted_records = sorted(self.records, key=lambda r: r.state)
        
        result = {}
        for state, group in groupby(sorted_records, key=lambda r: r.state):
            profit_sum = reduce(lambda acc, r: acc + r.profit, group, 0.0)
            result[state] = profit_sum
        
        return result
    
    def top_states_by_sales(self, n: int = 5) -> List[Tuple[str, float]]:
        """Find top N states by total sales."""
        sales_by_state = self.sales_by_state()
        if not sales_by_state:
            return []
        
        sorted_states = sorted(sales_by_state.items(), key=lambda x: x[1], reverse=True)
        return sorted_states[:n]
    
    def top_states_by_profit(self, n: int = 5) -> List[Tuple[str, float]]:
        """Find top N states by total profit."""
        profit_by_state = self.profit_by_state()
        if not profit_by_state:
            return []
        
        sorted_states = sorted(profit_by_state.items(), key=lambda x: x[1], reverse=True)
        return sorted_states[:n]
    
    # ==================== MULTI-LEVEL GROUPING ====================
    
    def sales_by_region_and_category(self) -> Dict[str, Dict[str, float]]:
        """Calculate sales grouped by region and category."""
        # Sort by region first, then category
        sorted_records = sorted(self.records, key=lambda r: (r.region, r.category))
        
        result = {}
        for (region, category), group in groupby(sorted_records, key=lambda r: (r.region, r.category)):
            if region not in result:
                result[region] = {}
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[region][category] = sales_sum
        
        return result
    
    def profit_by_category_and_subcategory(self) -> Dict[str, Dict[str, float]]:
        """Calculate profit grouped by category and sub-category."""
        sorted_records = sorted(self.records, key=lambda r: (r.category, r.sub_category))
        
        result = {}
        for (category, subcategory), group in groupby(sorted_records, key=lambda r: (r.category, r.sub_category)):
            if category not in result:
                result[category] = {}
            profit_sum = reduce(lambda acc, r: acc + r.profit, group, 0.0)
            result[category][subcategory] = profit_sum
        
        return result
    
    def sales_by_segment_and_region(self) -> Dict[str, Dict[str, float]]:
        """Calculate sales grouped by segment and region."""
        sorted_records = sorted(self.records, key=lambda r: (r.segment, r.region))
        
        result = {}
        for (segment, region), group in groupby(sorted_records, key=lambda r: (r.segment, r.region)):
            if segment not in result:
                result[segment] = {}
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[segment][region] = sales_sum
        
        return result
    
    # ==================== TIME-BASED ANALYSIS ====================
    
    def sales_by_year(self) -> Dict[int, float]:
        """Calculate total sales grouped by year."""
        sorted_records = sorted(self.records, key=lambda r: r.get_year())
        
        result = {}
        for year, group in groupby(sorted_records, key=lambda r: r.get_year()):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[year] = sales_sum
        
        return result
    
    def sales_by_month(self) -> Dict[int, float]:
        """Calculate total sales grouped by month (across all years)."""
        sorted_records = sorted(self.records, key=lambda r: r.get_month())
        
        result = {}
        for month, group in groupby(sorted_records, key=lambda r: r.get_month()):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            result[month] = sales_sum
        
        return result
    
    def average_sales_by_year(self) -> Dict[int, float]:
        """Calculate average sales grouped by year."""
        sorted_records = sorted(self.records, key=lambda r: r.get_year())
        
        result = {}
        for year, group in groupby(sorted_records, key=lambda r: r.get_year()):
            group_list = list(group)
            if group_list:
                total = reduce(lambda acc, r: acc + r.sales, group_list, 0.0)
                result[year] = total / len(group_list)
        
        return result
    
    def sales_trend_by_year(self) -> List[Tuple[int, float]]:
        """Get sales trend over years (sorted by year)."""
        sales_by_year = self.sales_by_year()
        return sorted(sales_by_year.items(), key=lambda x: x[0])
    
    # ==================== ADVANCED OPERATIONS ====================
    
    def orders_with_high_discount(self, threshold: float = 0.2) -> List[SalesRecord]:
        """Find orders with discount greater than threshold.
        
        Functional programming: Uses filter() with lambda expression to select records.
        """
        return list(filter(lambda r: r.discount > threshold, self.records))
    
    def count_high_discount_orders(self, threshold: float = 0.2) -> int:
        """Count orders with discount greater than threshold."""
        return len(self.orders_with_high_discount(threshold))
    
    def profit_margins(self) -> List[float]:
        """Calculate profit margins for all records.
        
        Functional programming: Uses map() with lambda to transform each record.
        """
        return list(map(lambda r: r.get_profit_margin(), self.records))
    
    def average_profit_margin(self) -> float:
        """Calculate average profit margin."""
        margins = self.profit_margins()
        if not margins:
            return 0.0
        return reduce(lambda acc, m: acc + m, margins, 0.0) / len(margins)
    
    def top_products_by_sales(self, n: int = 10) -> List[Tuple[str, float]]:
        """Find top N products by total sales."""
        # Group by product name
        sorted_records = sorted(self.records, key=lambda r: r.product_name)
        
        product_sales = {}
        for product, group in groupby(sorted_records, key=lambda r: r.product_name):
            sales_sum = reduce(lambda acc, r: acc + r.sales, group, 0.0)
            product_sales[product] = sales_sum
        
        if not product_sales:
            return []
        
        sorted_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)
        return sorted_products[:n]
    
    def products_with_negative_profit(self) -> List[SalesRecord]:
        """Find all products with negative profit."""
        return list(filter(lambda r: r.profit < 0, self.records))
    
    def count_negative_profit_orders(self) -> int:
        """Count orders with negative profit."""
        return len(self.products_with_negative_profit())

