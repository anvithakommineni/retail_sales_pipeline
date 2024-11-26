import pandas as pd
import numpy as np
import logging
from datetime import datetime
import json
import os

class DataQualityChecker:
    def __init__(self):
        self.log_dir = '../logs'
        self.quality_report_path = os.path.join(self.log_dir, 'data_quality_reports')
        os.makedirs(self.quality_report_path, exist_ok=True)
        
        # Data quality rules
        self.rules = {
            'orders': {
                'required_columns': ['order_id', 'customer_id', 'order_status', 
                                   'order_purchase_timestamp'],
                'unique_columns': ['order_id'],
                'non_null_columns': ['order_id', 'customer_id', 'order_status'],
                'date_columns': ['order_purchase_timestamp', 'order_delivered_customer_date'],
                'valid_statuses': ['delivered', 'shipped', 'canceled', 'processing']
            },
            'order_items': {
                'required_columns': ['order_id', 'product_id', 'price', 'freight_value'],
                'non_null_columns': ['order_id', 'product_id', 'price'],
                'numeric_ranges': {
                    'price': (0, 10000),
                    'freight_value': (0, 1000)
                }
            },
            'customers': {
                'required_columns': ['customer_id', 'customer_city', 'customer_state'],
                'unique_columns': ['customer_id'],
                'non_null_columns': ['customer_id']
            }
        }

    def check_data_quality(self, df: pd.DataFrame, dataset_name: str) -> dict:
        """
        Perform comprehensive data quality checks on a dataset
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report = {
            'dataset': dataset_name,
            'timestamp': timestamp,
            'total_rows': len(df),
            'issues': [],
            'summary': {},
            'passed': True
        }

        try:
            # 1. Check required columns
            missing_columns = set(self.rules[dataset_name]['required_columns']) - set(df.columns)
            if missing_columns:
                report['issues'].append(f"Missing required columns: {missing_columns}")
                report['passed'] = False

            # 2. Check for null values in required fields
            for col in self.rules[dataset_name].get('non_null_columns', []):
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    report['issues'].append(f"Found {null_count} null values in {col}")
                    report['passed'] = False

            # 3. Check unique constraints
            for col in self.rules[dataset_name].get('unique_columns', []):
                duplicate_count = df[col].duplicated().sum()
                if duplicate_count > 0:
                    report['issues'].append(f"Found {duplicate_count} duplicate values in {col}")
                    report['passed'] = False

            # 4. Check numeric ranges
            for col, (min_val, max_val) in self.rules[dataset_name].get('numeric_ranges', {}).items():
                out_of_range = df[
                    (df[col] < min_val) | (df[col] > max_val)
                ].shape[0]
                if out_of_range > 0:
                    report['issues'].append(
                        f"Found {out_of_range} values outside range ({min_val}, {max_val}) in {col}"
                    )
                    report['passed'] = False

            # 5. Check date validations
            for col in self.rules[dataset_name].get('date_columns', []):
                try:
                    df[col] = pd.to_datetime(df[col])
                    future_dates = df[df[col] > datetime.now()].shape[0]
                    if future_dates > 0:
                        report['issues'].append(f"Found {future_dates} future dates in {col}")
                        report['passed'] = False
                except Exception as e:
                    report['issues'].append(f"Error parsing dates in {col}: {str(e)}")
                    report['passed'] = False

            # 6. Generate summary statistics
            report['summary'] = {
                'null_counts': df.isnull().sum().to_dict(),
                'unique_counts': df.nunique().to_dict(),
                'numeric_columns_stats': df.describe().to_dict()
            }

            # Save report
            self._save_quality_report(report)
            
            return report

        except Exception as e:
            report['issues'].append(f"Error in quality check: {str(e)}")
            report['passed'] = False
            return report

    def _save_quality_report(self, report: dict):
        """Save quality report to file"""
        filename = f"quality_report_{report['dataset']}_{report['timestamp']}.json"
        filepath = os.path.join(self.quality_report_path, filename)
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=4, default=str)