#!/usr/bin/env python3

import sys
from pathlib import Path
from sales_analysis.csv_reader import read_sales_data
from sales_analysis.analyzer import SalesAnalyzer


def format_currency(amount: float) -> str:
    """Format a number as currency."""
    return f"${amount:,.2f}"


def format_percentage(value: float) -> str:
    """Format a number as percentage."""
    return f"{value:.2f}%"


def print_section(title: str):
    """Print a section header."""
    print("\n" + "=" * 70)
    print(f"=== {title} ===")
    print("=" * 70)


def print_dict_results(title: str, results: dict, format_func=format_currency):
    """Print dictionary results in a formatted table."""
    print(f"\n{title}:")
    print("-" * 70)
    if not results:
        print("No data available")
        return
    
    for key, value in sorted(results.items()):
        # Handle both string and integer keys
        key_str = str(key) if not isinstance(key, str) else key
        print(f"  {key_str:30s}: {format_func(value)}")
    
    # Print total if applicable
    if isinstance(list(results.values())[0], (int, float)):
        total = sum(results.values())
        print("-" * 70)
        print(f"  {'Total':30s}: {format_func(total)}")


def print_list_results(title: str, results: list, format_func=format_currency):
    print(f"\n{title}:")
    print("-" * 70)
    if not results:
        print("No data available")
        return
    
    for i, (key, value) in enumerate(results, 1):
        key_str = str(key) if not isinstance(key, str) else key
        print(f"  {i:2d}. {key_str:40s}: {format_func(value)}")


def print_multi_level_results(title: str, results: dict):
    """Print multi-level grouped results."""
    print(f"\n{title}:")
    print("-" * 70)
    if not results:
        print("No data available")
        return
    
    for key1, inner_dict in sorted(results.items()):
        print(f"\n  {key1}:")
        for key2, value in sorted(inner_dict.items()):
            print(f"    {key2:30s}: {format_currency(value)}")


def run_all_analyses(csv_path: str):
    try:
        print("Loading sales data...")
        records = read_sales_data(csv_path)
        print(f"Loaded {len(records)} sales records")
        
        analyzer = SalesAnalyzer(records)
        
        # ==================== BASIC AGGREGATIONS ====================
        print_section("Basic Aggregations")
        
        print(f"\nTotal Sales:        {format_currency(analyzer.total_sales())}")
        print(f"Total Profit:       {format_currency(analyzer.total_profit())}")
        print(f"Average Sales:      {format_currency(analyzer.average_sales())}")
        print(f"Average Profit:     {format_currency(analyzer.average_profit())}")
        print(f"Total Quantity:    {analyzer.total_quantity():,}")
        print(f"Average Discount:   {format_percentage(analyzer.average_discount() * 100)}")
        print(f"Maximum Sales:      {format_currency(analyzer.max_sales())}")
        print(f"Minimum Profit:     {format_currency(analyzer.min_profit())}")
        
        # ==================== GROUPING BY REGION ====================
        print_section("Analysis by Region")
        
        print_dict_results("Total Sales by Region", analyzer.sales_by_region())
        print_dict_results("Total Profit by Region", analyzer.profit_by_region())
        print_dict_results("Average Sales by Region", analyzer.average_sales_by_region())
        print_dict_results("Order Count by Region", analyzer.order_count_by_region(), 
                          format_func=lambda x: f"{x:,}")
        
        top_region, top_sales = analyzer.top_region_by_sales()
        print(f"\nTop Region by Sales: {top_region} ({format_currency(top_sales)})")
        
        # ==================== GROUPING BY CATEGORY ====================
        print_section("Analysis by Product Category")
        
        print_dict_results("Total Sales by Category", analyzer.sales_by_category())
        print_dict_results("Total Profit by Category", analyzer.profit_by_category())
        print_dict_results("Average Sales by Category", analyzer.average_sales_by_category())
        print_dict_results("Product Count by Category", analyzer.product_count_by_category(),
                          format_func=lambda x: f"{x:,}")
        
        top_category, top_profit = analyzer.top_category_by_profit()
        print(f"\nTop Category by Profit: {top_category} ({format_currency(top_profit)})")
        
        # ==================== GROUPING BY SEGMENT ====================
        print_section("Analysis by Customer Segment")
        
        print_dict_results("Total Sales by Segment", analyzer.sales_by_segment())
        print_dict_results("Total Profit by Segment", analyzer.profit_by_segment())
        print_dict_results("Average Sales by Segment", analyzer.average_sales_by_segment())
        print_dict_results("Customer Count by Segment", analyzer.customer_count_by_segment(),
                          format_func=lambda x: f"{x:,}")
        
        top_segment, segment_profit = analyzer.most_profitable_segment()
        print(f"\nMost Profitable Segment: {top_segment} ({format_currency(segment_profit)})")
        
        # ==================== GROUPING BY STATE ====================
        print_section("Analysis by State")
        
        print_dict_results("Total Sales by State", analyzer.sales_by_state())
        print_dict_results("Total Profit by State", analyzer.profit_by_state())
        
        print_list_results("Top 5 States by Sales", analyzer.top_states_by_sales(5))
        print_list_results("Top 5 States by Profit", analyzer.top_states_by_profit(5))
        
        # ==================== MULTI-LEVEL GROUPING ====================
        print_section("Multi-Level Grouping Analysis")
        
        print_multi_level_results("Sales by Region and Category", 
                                 analyzer.sales_by_region_and_category())
        print_multi_level_results("Profit by Category and Sub-Category",
                                 analyzer.profit_by_category_and_subcategory())
        print_multi_level_results("Sales by Segment and Region",
                                 analyzer.sales_by_segment_and_region())
        
        # ==================== TIME-BASED ANALYSIS ====================
        print_section("Time-Based Analysis")
        
        print_dict_results("Total Sales by Year", analyzer.sales_by_year())
        print_dict_results("Average Sales by Year", analyzer.average_sales_by_year())
        
        # Sales by month
        sales_by_month = analyzer.sales_by_month()
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        print("\nTotal Sales by Month:")
        print("-" * 70)
        for month_num in sorted(sales_by_month.keys()):
            month_name = month_names.get(month_num, f"Month {month_num}")
            print(f"  {month_name:20s}: {format_currency(sales_by_month[month_num])}")
        
        print_list_results("Sales Trend by Year (Chronological)", 
                          analyzer.sales_trend_by_year())
        
        # ==================== ADVANCED OPERATIONS ====================
        print_section("Advanced Analysis")
        
        high_discount_count = analyzer.count_high_discount_orders(0.2)
        print(f"\nOrders with Discount > 20%: {high_discount_count:,}")
        
        avg_margin = analyzer.average_profit_margin()
        print(f"Average Profit Margin: {format_percentage(avg_margin)}")
        
        print_list_results("Top 10 Products by Sales", analyzer.top_products_by_sales(10))
        
        negative_profit_count = analyzer.count_negative_profit_orders()
        print(f"\nOrders with Negative Profit: {negative_profit_count:,}")
        
        print("\n" + "=" * 70)
        print("Analysis Complete!")
        print("=" * 70)
        
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error during analysis: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    # Check for interactive mode
    if len(sys.argv) > 1 and sys.argv[1] in ["--interactive", "-i"]:
        # Import and run interactive CLI
        from interactive_cli import InteractiveCLI
        from sales_analysis.csv_reader import read_sales_data
        from sales_analysis.analyzer import SalesAnalyzer
        
        csv_path = sys.argv[2] if len(sys.argv) > 2 else str(Path(__file__).parent / "sales_data.csv")
        
        if not Path(csv_path).exists():
            print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        
        try:
            print("Loading sales data...")
            records = read_sales_data(csv_path)
            print(f"Loaded {len(records)} sales records")
            
            analyzer = SalesAnalyzer(records)
            cli = InteractiveCLI(analyzer, len(records))
            cli.run()
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
        return
    
    # Regular mode - run all analyses
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = str(Path(__file__).parent / "sales_data.csv") # read files data
    
    if not Path(csv_path).exists():
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        print(f"Usage: python main.py [path_to_csv_file]", file=sys.stderr)
        print(f"       python main.py --interactive [path_to_csv_file]", file=sys.stderr)
        sys.exit(1)
    
    run_all_analyses(csv_path)


if __name__ == "__main__":
    main()

