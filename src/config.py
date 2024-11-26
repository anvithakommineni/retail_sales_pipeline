import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Input file paths
INPUT_FILES = {
    'orders': os.path.join(RAW_DIR, 'olist_orders_dataset.csv'),
    'order_items': os.path.join(RAW_DIR, 'olist_order_items_dataset.csv'),
    'products': os.path.join(RAW_DIR, 'olist_products_dataset.csv'),
    'customers': os.path.join(RAW_DIR, 'olist_customers_dataset.csv'),
    'payments': os.path.join(RAW_DIR, 'olist_order_payments_dataset.csv'),
    'reviews': os.path.join(RAW_DIR, 'olist_order_reviews_dataset.csv'),
    'sellers': os.path.join(RAW_DIR, 'olist_sellers_dataset.csv'),
    'category_translation': os.path.join(RAW_DIR, 'product_category_name_translation.csv')
}

# Output file paths
OUTPUT_FILES = {
    'sales_summary': os.path.join(PROCESSED_DIR, 'sales_summary.csv'),
    'customer_metrics': os.path.join(PROCESSED_DIR, 'customer_metrics.csv'),
    'product_performance': os.path.join(PROCESSED_DIR, 'product_performance.csv')
}

# Create necessary directories
for directory in [DATA_DIR, RAW_DIR, PROCESSED_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)