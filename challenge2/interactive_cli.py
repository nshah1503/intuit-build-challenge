#!/usr/bin/env python3

"""
Interactive CLI for Sales Data Analysis.
Provides an interactive command-line interface to explore sales data.
"""

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
        key_str = str(key) if not isinstance(key, str) else key
        print(f"  {key_str:30s}: {format_func(value)}")
    
    if isinstance(list(results.values())[0], (int, float)):
        total = sum(results.values())
        print("-" * 70)
        print(f"  {'Total':30s}: {format_func(total)}")


def print_list_results(title: str, results: list, format_func=format_currency):
    """Print list results in a formatted table."""
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


class InteractiveCLI:
    """Interactive CLI handler for sales analysis."""
    
    def __init__(self, analyzer: SalesAnalyzer, record_count: int):
        self.analyzer = analyzer
        self.record_count = record_count
        self.running = True
        
    def print_welcome(self):
        """Print welcome message and help."""
        print("\n" + "=" * 70)
        print("=== Sales Data Analysis - Interactive CLI ===")
        print("=" * 70)
        print(f"\nLoaded {self.record_count:,} sales records")
        print("\nType 'help' to see available commands")
        print("Type 'quit' or 'exit' to exit\n")
    
    def print_help(self):
        """Print help message with all available commands."""
        print("\n" + "=" * 70)
        print("=== Available Commands ===")
        print("=" * 70)
        print("\n Dataset Overview:")
        print("  dataset summary          - Basic aggregations (total sales, profit, etc.)")
        print("  summary                  - Alias for 'dataset summary'")
        
        print("\n Region Analysis:")
        print("  region                   - All region analysis")
        print("  region sales              - Sales by region")
        print("  region profit             - Profit by region")
        print("  region top                - Top region by sales")
        
        print("\n Category Analysis:")
        print("  category                 - All category analysis")
        print("  category sales            - Sales by category")
        print("  category profit           - Profit by category")
        print("  category top              - Top category by profit")
        
        print("\n Segment Analysis:")
        print("  segment                  - All segment analysis")
        print("  segment sales             - Sales by segment")
        print("  segment profit            - Profit by segment")
        print("  segment top               - Most profitable segment")
        
        print("\n State Analysis:")
        print("  state                    - All state analysis")
        print("  state sales               - Sales by state")
        print("  state profit              - Profit by state")
        print("  state top                 - Top 5 states by sales and profit")
        
        print("\n Time Analysis:")
        print("  time                     - All time-based analysis")
        print("  time year                 - Sales by year")
        print("  time month                - Sales by month")
        print("  time trend                - Sales trend by year")
        
        print("\n Multi-Level Analysis:")
        print("  multi                    - All multi-level grouping")
        print("  multi region-category     - Sales by region and category")
        print("  multi category-sub        - Profit by category and sub-category")
        print("  multi segment-region      - Sales by segment and region")
        
        print("\n Advanced Analysis:")
        print("  advanced                 - All advanced operations")
        print("  advanced discounts        - High discount orders")
        print("  advanced margin           - Profit margin analysis")
        print("  advanced products         - Top products by sales")
        print("  advanced negative         - Negative profit analysis")
        
        print("\n Other Commands:")
        print("  all                      - Run all analyses (full report)")
        print("  help                     - Show this help message")
        print("  quit / exit              - Exit the CLI")
        print("\n" + "=" * 70 + "\n")
    
    def handle_dataset_summary(self):
        """Handle dataset summary command."""
        print_section("Basic Aggregations")
        
        print(f"\nTotal Sales:        {format_currency(self.analyzer.total_sales())}")
        print(f"Total Profit:       {format_currency(self.analyzer.total_profit())}")
        print(f"Average Sales:      {format_currency(self.analyzer.average_sales())}")
        print(f"Average Profit:     {format_currency(self.analyzer.average_profit())}")
        print(f"Total Quantity:    {self.analyzer.total_quantity():,}")
        print(f"Average Discount:   {format_percentage(self.analyzer.average_discount() * 100)}")
        print(f"Maximum Sales:      {format_currency(self.analyzer.max_sales())}")
        print(f"Minimum Profit:     {format_currency(self.analyzer.min_profit())}")
        print()
    
    def handle_region(self, args: list):
        """Handle region analysis commands."""
        if not args:
            print_section("Analysis by Region")
            print_dict_results("Total Sales by Region", self.analyzer.sales_by_region())
            print_dict_results("Total Profit by Region", self.analyzer.profit_by_region())
            print_dict_results("Average Sales by Region", self.analyzer.average_sales_by_region())
            print_dict_results("Order Count by Region", self.analyzer.order_count_by_region(),
                            format_func=lambda x: f"{x:,}")
            top_region, top_sales = self.analyzer.top_region_by_sales()
            print(f"\nTop Region by Sales: {top_region} ({format_currency(top_sales)})")
        elif args[0] == "sales":
            print_dict_results("Total Sales by Region", self.analyzer.sales_by_region())
        elif args[0] == "profit":
            print_dict_results("Total Profit by Region", self.analyzer.profit_by_region())
        elif args[0] == "top":
            top_region, top_sales = self.analyzer.top_region_by_sales()
            print(f"\nTop Region by Sales: {top_region} ({format_currency(top_sales)})")
        else:
            print(f"Unknown region subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_category(self, args: list):
        """Handle category analysis commands."""
        if not args:
            print_section("Analysis by Product Category")
            print_dict_results("Total Sales by Category", self.analyzer.sales_by_category())
            print_dict_results("Total Profit by Category", self.analyzer.profit_by_category())
            print_dict_results("Average Sales by Category", self.analyzer.average_sales_by_category())
            print_dict_results("Product Count by Category", self.analyzer.product_count_by_category(),
                            format_func=lambda x: f"{x:,}")
            top_category, top_profit = self.analyzer.top_category_by_profit()
            print(f"\nTop Category by Profit: {top_category} ({format_currency(top_profit)})")
        elif args[0] == "sales":
            print_dict_results("Total Sales by Category", self.analyzer.sales_by_category())
        elif args[0] == "profit":
            print_dict_results("Total Profit by Category", self.analyzer.profit_by_category())
        elif args[0] == "top":
            top_category, top_profit = self.analyzer.top_category_by_profit()
            print(f"\nTop Category by Profit: {top_category} ({format_currency(top_profit)})")
        else:
            print(f"Unknown category subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_segment(self, args: list):
        """Handle segment analysis commands."""
        if not args:
            print_section("Analysis by Customer Segment")
            print_dict_results("Total Sales by Segment", self.analyzer.sales_by_segment())
            print_dict_results("Total Profit by Segment", self.analyzer.profit_by_segment())
            print_dict_results("Average Sales by Segment", self.analyzer.average_sales_by_segment())
            print_dict_results("Customer Count by Segment", self.analyzer.customer_count_by_segment(),
                            format_func=lambda x: f"{x:,}")
            top_segment, segment_profit = self.analyzer.most_profitable_segment()
            print(f"\nMost Profitable Segment: {top_segment} ({format_currency(segment_profit)})")
        elif args[0] == "sales":
            print_dict_results("Total Sales by Segment", self.analyzer.sales_by_segment())
        elif args[0] == "profit":
            print_dict_results("Total Profit by Segment", self.analyzer.profit_by_segment())
        elif args[0] == "top":
            top_segment, segment_profit = self.analyzer.most_profitable_segment()
            print(f"\nMost Profitable Segment: {top_segment} ({format_currency(segment_profit)})")
        else:
            print(f"Unknown segment subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_state(self, args: list):
        """Handle state analysis commands."""
        if not args:
            print_section("Analysis by State")
            print_dict_results("Total Sales by State", self.analyzer.sales_by_state())
            print_dict_results("Total Profit by State", self.analyzer.profit_by_state())
            print_list_results("Top 5 States by Sales", self.analyzer.top_states_by_sales(5))
            print_list_results("Top 5 States by Profit", self.analyzer.top_states_by_profit(5))
        elif args[0] == "sales":
            print_dict_results("Total Sales by State", self.analyzer.sales_by_state())
        elif args[0] == "profit":
            print_dict_results("Total Profit by State", self.analyzer.profit_by_state())
        elif args[0] == "top":
            print_list_results("Top 5 States by Sales", self.analyzer.top_states_by_sales(5))
            print_list_results("Top 5 States by Profit", self.analyzer.top_states_by_profit(5))
        else:
            print(f"Unknown state subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_time(self, args: list):
        """Handle time-based analysis commands."""
        if not args:
            print_section("Time-Based Analysis")
            print_dict_results("Total Sales by Year", self.analyzer.sales_by_year())
            print_dict_results("Average Sales by Year", self.analyzer.average_sales_by_year())
            
            sales_by_month = self.analyzer.sales_by_month()
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
                            self.analyzer.sales_trend_by_year())
        elif args[0] == "year":
            print_dict_results("Total Sales by Year", self.analyzer.sales_by_year())
            print_dict_results("Average Sales by Year", self.analyzer.average_sales_by_year())
        elif args[0] == "month":
            sales_by_month = self.analyzer.sales_by_month()
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
        elif args[0] == "trend":
            print_list_results("Sales Trend by Year (Chronological)", 
                            self.analyzer.sales_trend_by_year())
        else:
            print(f"Unknown time subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_multi(self, args: list):
        """Handle multi-level grouping commands."""
        if not args:
            print_section("Multi-Level Grouping Analysis")
            print_multi_level_results("Sales by Region and Category", 
                                    self.analyzer.sales_by_region_and_category())
            print_multi_level_results("Profit by Category and Sub-Category",
                                    self.analyzer.profit_by_category_and_subcategory())
            print_multi_level_results("Sales by Segment and Region",
                                    self.analyzer.sales_by_segment_and_region())
        elif args[0] == "region-category":
            print_multi_level_results("Sales by Region and Category", 
                                    self.analyzer.sales_by_region_and_category())
        elif args[0] == "category-sub":
            print_multi_level_results("Profit by Category and Sub-Category",
                                    self.analyzer.profit_by_category_and_subcategory())
        elif args[0] == "segment-region":
            print_multi_level_results("Sales by Segment and Region",
                                    self.analyzer.sales_by_segment_and_region())
        else:
            print(f"Unknown multi subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_advanced(self, args: list):
        """Handle advanced analysis commands."""
        if not args:
            print_section("Advanced Analysis")
            high_discount_count = self.analyzer.count_high_discount_orders(0.2)
            print(f"\nOrders with Discount > 20%: {high_discount_count:,}")
            
            avg_margin = self.analyzer.average_profit_margin()
            print(f"Average Profit Margin: {format_percentage(avg_margin)}")
            
            print_list_results("Top 10 Products by Sales", self.analyzer.top_products_by_sales(10))
            
            negative_profit_count = self.analyzer.count_negative_profit_orders()
            print(f"\nOrders with Negative Profit: {negative_profit_count:,}")
        elif args[0] == "discounts":
            high_discount_count = self.analyzer.count_high_discount_orders(0.2)
            print(f"\nOrders with Discount > 20%: {high_discount_count:,}")
        elif args[0] == "margin":
            avg_margin = self.analyzer.average_profit_margin()
            print(f"\nAverage Profit Margin: {format_percentage(avg_margin)}")
        elif args[0] == "products":
            print_list_results("Top 10 Products by Sales", self.analyzer.top_products_by_sales(10))
        elif args[0] == "negative":
            negative_profit_count = self.analyzer.count_negative_profit_orders()
            print(f"\nOrders with Negative Profit: {negative_profit_count:,}")
        else:
            print(f"Unknown advanced subcommand: {args[0]}. Use 'help' for available commands.")
        print()
    
    def handle_all(self):
        """Handle 'all' command - run complete analysis."""
        self.handle_dataset_summary()
        self.handle_region([])
        self.handle_category([])
        self.handle_segment([])
        self.handle_state([])
        self.handle_multi([])
        self.handle_time([])
        self.handle_advanced([])
        print("=" * 70)
        print("Analysis Complete!")
        print("=" * 70 + "\n")
    
    def process_query(self, command: str):
        """Process a user command."""
        parts = command.strip().lower().split()
        if not parts:
            return
        
        cmd = parts[0]
        args = parts[1:] if len(parts) > 1 else []
        
        if cmd in ["quit", "exit"]:
            self.running = False
            print("\nGoodbye!\n")
        elif cmd == "help":
            self.print_help()
        elif cmd in ["dataset", "summary"]:
            self.handle_dataset_summary()
        elif cmd == "region":
            self.handle_region(args)
        elif cmd == "category":
            self.handle_category(args)
        elif cmd == "segment":
            self.handle_segment(args)
        elif cmd == "state":
            self.handle_state(args)
        elif cmd == "time":
            self.handle_time(args)
        elif cmd == "multi":
            self.handle_multi(args)
        elif cmd == "advanced":
            self.handle_advanced(args)
        elif cmd == "all":
            self.handle_all()
        else:
            print(f"Unknown command: '{cmd}'. Type 'help' for available commands.\n")
    
    def run(self):
        """Run the interactive CLI loop."""
        self.print_welcome()
        
        while self.running:
            try:
                command = input("sales> ").strip()
                if command:
                    self.process_query(command)
            except KeyboardInterrupt: # system call
                print("\n\nGoodbye!\n")
                break
            except EOFError: # system call
                print("\n\nGoodbye!\n")
                break


def main():
    """Main entry point for interactive CLI."""
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = str(Path(__file__).parent / "sales_data.csv")
    
    if not Path(csv_path).exists():
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        print(f"Usage: python interactive_cli.py [path_to_csv_file]", file=sys.stderr)
        sys.exit(1)
    
    try:
        print("Loading sales data...")
        records = read_sales_data(csv_path)
        print(f"Loaded {len(records)} sales records")
        
        analyzer = SalesAnalyzer(records)
        cli = InteractiveCLI(analyzer, len(records))
        cli.run()
        
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error during analysis: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

