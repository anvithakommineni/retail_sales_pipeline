import pandas as pd
import logging
from datetime import datetime
from config import INPUT_FILES  # Add this import
from data_quality import DataQualityChecker
from data_governance import DataGovernance

class EcommercePipeline:
    def __init__(self):
        self.quality_checker = DataQualityChecker()
        self.governance = DataGovernance()
        
    def extract_data(self):
        """Extract and validate raw data"""
        for dataset_name, filepath in INPUT_FILES.items():
            # Load data
            df = pd.read_csv(filepath)
            
            # Perform quality checks
            quality_report = self.quality_checker.check_data_quality(df, dataset_name)
            if not quality_report['passed']:
                logging.error(f"Data quality issues in {dataset_name}: {quality_report['issues']}")
                raise ValueError(f"Data quality check failed for {dataset_name}")
            
            # Track dataset
            self.governance.track_dataset(df, dataset_name, 'raw')
            
            self.data[dataset_name] = df

    def transform_data(self):
        """Transform data with quality checks and governance"""
        try:
            # Example transformation with tracking
            sales_df = self.data['orders'].merge(self.data['order_items'], on='order_id')
            
            self.governance.log_transformation(
                source_dataset='orders,order_items',
                target_dataset='sales',
                transformation_type='merge',
                transformation_details={'merge_key': 'order_id'}
            )
            
            # Quality check on transformed data
            quality_report = self.quality_checker.check_data_quality(sales_df, 'sales')
            if not quality_report['passed']:
                logging.error(f"Quality issues in transformed data: {quality_report['issues']}")
                raise ValueError("Transform quality check failed")
            
            # Track transformed dataset
            self.governance.track_dataset(sales_df, 'sales', 'transformed')
            
            # Continue with other transformations...

        except Exception as e:
            logging.error(f"Error in transformation: {str(e)}")
            raise