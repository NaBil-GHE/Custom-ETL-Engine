#!/usr/bin/env python3
"""Generate sample e-commerce sales dataset"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Set seed for reproducibility
np.random.seed(42)

# Generate 1000 sample sales records
n_records = 1000

# Date range: last 2 years
start_date = datetime(2024, 1, 1)
dates = [start_date + timedelta(days=int(x)) for x in np.random.randint(0, 730, n_records)]

# Customer data
customer_ids = [f'C{i:04d}' for i in range(1, 201)]
customer_names = ['Ahmed Ali', 'Fatima Hassan', 'Mohamed Saidi', 'Sara Benali', 
                  'Youssef Mansour', 'Amina Kaddour', 'Omar Belkacem', 'Nadia Brahimi',
                  'Karim Bouazza', 'Leila Hamdi', 'Rachid Slimani', 'Samira Djebar',
                  'Bilal Cherif', 'Hanane Mekki', 'Tarek Ould', 'Zohra Benmoussa',
                  'John Smith', 'Emma Johnson', 'James Wilson', 'Olivia Brown'] * 10

countries = ['Algeria', 'France', 'Morocco', 'Tunisia', 'USA', 'UK', 'Germany', 'Spain']
cities_map = {
    'Algeria': ['Algiers', 'Oran', 'Constantine', 'Tlemcen'],
    'France': ['Paris', 'Lyon', 'Marseille'],
    'Morocco': ['Casablanca', 'Rabat', 'Marrakech'],
    'Tunisia': ['Tunis', 'Sfax', 'Sousse'],
    'USA': ['New York', 'Los Angeles', 'Chicago'],
    'UK': ['London', 'Manchester', 'Birmingham'],
    'Germany': ['Berlin', 'Munich', 'Hamburg'],
    'Spain': ['Madrid', 'Barcelona', 'Valencia']
}

# Product data
product_ids = [f'P{i:04d}' for i in range(1, 101)]
products = [
    ('Laptop Dell XPS 15', 'Electronics', 'Computers', 'Dell'),
    ('iPhone 15 Pro', 'Electronics', 'Phones', 'Apple'),
    ('Samsung Galaxy S24', 'Electronics', 'Phones', 'Samsung'),
    ('Sony Headphones WH-1000XM5', 'Electronics', 'Audio', 'Sony'),
    ('Nike Air Max', 'Fashion', 'Shoes', 'Nike'),
    ('Adidas Running Shoes', 'Fashion', 'Shoes', 'Adidas'),
    ('Levis Jeans 501', 'Fashion', 'Clothing', 'Levis'),
    ('Zara Jacket', 'Fashion', 'Clothing', 'Zara'),
    ('Coffee Maker Deluxe', 'Home', 'Kitchen', 'Generic'),
    ('Vacuum Cleaner Robot', 'Home', 'Appliances', 'iRobot'),
    ('Harry Potter Book Set', 'Books', 'Fiction', 'Scholastic'),
    ('Python Programming Guide', 'Books', 'Tech', 'OReilly'),
    ('Office Chair Ergonomic', 'Furniture', 'Office', 'Herman Miller'),
    ('Standing Desk', 'Furniture', 'Office', 'IKEA'),
    ('Yoga Mat Premium', 'Sports', 'Fitness', 'Manduka'),
    ('Bicycle Mountain Bike', 'Sports', 'Cycling', 'Trek'),
    ('Smartwatch Apple Watch', 'Electronics', 'Wearables', 'Apple'),
    ('Kindle Paperwhite', 'Electronics', 'E-readers', 'Amazon'),
    ('Camera Canon EOS R6', 'Electronics', 'Cameras', 'Canon'),
    ('Gaming Console PS5', 'Electronics', 'Gaming', 'Sony')
] * 5

# Generate records
records = []
for i in range(n_records):
    customer_id = np.random.choice(customer_ids)
    customer_idx = int(customer_id[1:]) - 1
    customer_name = customer_names[customer_idx % len(customer_names)]
    
    country = np.random.choice(countries, p=[0.3, 0.15, 0.15, 0.1, 0.1, 0.08, 0.07, 0.05])
    city = np.random.choice(cities_map[country])
    
    # Create email from customer name
    email_name = customer_name.lower().replace(' ', '.')
    email = f"{email_name}@email.com"
    
    product_idx = np.random.randint(0, len(products))
    product_id = product_ids[product_idx % len(product_ids)]
    product_name, category, subcategory, brand = products[product_idx]
    
    quantity = np.random.randint(1, 6)
    unit_price = round(np.random.uniform(10, 2000), 2)
    discount = round(np.random.choice([0, 0, 0, 5, 10, 15, 20], p=[0.5, 0.2, 0.1, 0.08, 0.07, 0.03, 0.02]), 2)
    total_amount = round(quantity * unit_price * (1 - discount/100), 2)
    
    records.append({
        'order_id': f'ORD{i+1:06d}',
        'order_date': dates[i].strftime('%Y-%m-%d'),
        'customer_id': customer_id,
        'customer_name': customer_name,
        'email': email,
        'country': country,
        'city': city,
        'product_id': product_id,
        'product_name': product_name,
        'category': category,
        'subcategory': subcategory,
        'brand': brand,
        'quantity': quantity,
        'unit_price': unit_price,
        'discount': discount,
        'total_amount': total_amount
    })

# Create DataFrame
df = pd.DataFrame(records)

# Ensure data/raw directory exists
os.makedirs('data/raw', exist_ok=True)

# Save to CSV
df.to_csv('data/raw/ecommerce_sales.csv', index=False)
print(f'✓ Generated {len(df)} sample e-commerce sales records')
print(f'  Date range: {df["order_date"].min()} to {df["order_date"].max()}')
print(f'  Unique customers: {df["customer_id"].nunique()}')
print(f'  Unique products: {df["product_id"].nunique()}')
print(f'  Categories: {", ".join(df["category"].unique())}')
print(f'  Total revenue: ${df["total_amount"].sum():,.2f}')
print(f'\nSample records:')
print(df.head(3).to_string(index=False))
