from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SalesRecord:    
    row_id: int
    order_id: str
    order_date: datetime
    ship_date: datetime
    ship_mode: str
    customer_id: str
    customer_name: str
    segment: str
    country: str
    city: str
    state: str
    postal_code: int
    region: str
    product_id: str
    category: str
    sub_category: str
    product_name: str
    sales: float
    quantity: int
    discount: float
    profit: float
    
    @classmethod
    def from_csv_row(cls, row: list, headers: list) -> 'SalesRecord':
        data = dict(zip(headers, row))
        
        # Try YYYY-MM-DD first, then M/D/YYYY
        date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']
        order_date = None
        ship_date = None
        
        for fmt in date_formats:
            try:
                order_date = datetime.strptime(data['Order Date'], fmt)
                ship_date = datetime.strptime(data['Ship Date'], fmt)
                break
            except ValueError:
                continue
        
        if order_date is None or ship_date is None:
            raise ValueError(f"Unable to parse dates: {data['Order Date']}, {data['Ship Date']}")
        
        return cls(
            row_id=int(data['Row ID']),
            order_id=data['Order ID'],
            order_date=order_date,
            ship_date=ship_date,
            ship_mode=data['Ship Mode'],
            customer_id=data['Customer ID'],
            customer_name=data['Customer Name'],
            segment=data['Segment'],
            country=data['Country'],
            city=data['City'],
            state=data['State'],
            postal_code=int(data['Postal Code']),
            region=data['Region'],
            product_id=data['Product ID'],
            category=data['Category'],
            sub_category=data['Sub-Category'],
            product_name=data['Product Name'],
            sales=float(data['Sales']),
            quantity=int(data['Quantity']),
            discount=float(data['Discount']),
            profit=float(data['Profit'])
        )
    
    def get_year(self) -> int:
        """Get the year from order date."""
        return self.order_date.year
    
    def get_month(self) -> int:
        """Get the month from order date."""
        return self.order_date.month
    
    def get_profit_margin(self) -> float:
        """Calculate profit margin (profit/sales)."""
        if self.sales == 0:
            return 0.0
        return (self.profit / self.sales) * 100

