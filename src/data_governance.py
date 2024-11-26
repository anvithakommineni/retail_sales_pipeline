import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import json
import os

class DataGovernance:
    def __init__(self):
        self.metadata_dir = '../logs/metadata'
        self.lineage_dir = '../logs/lineage'
        os.makedirs(self.metadata_dir, exist_ok=True)
        os.makedirs(self.lineage_dir, exist_ok=True)

    def track_dataset(self, df: pd.DataFrame, dataset_name: str, stage: str) -> dict:
        """
        Track dataset metadata and lineage
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create metadata
        metadata = {
            'dataset_name': dataset_name,
            'stage': stage,
            'timestamp': timestamp,
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'row_count': len(df),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'column_stats': {
                col: {
                    'null_count': df[col].isnull().sum(),
                    'unique_count': df[col].nunique()
                } for col in df.columns
            }
        }

        # Generate data fingerprint
        metadata['data_fingerprint'] = self._generate_fingerprint(df)
        
        # Save metadata
        self._save_metadata(metadata)
        
        # Track lineage
        self._track_lineage(dataset_name, stage, metadata)
        
        return metadata

    def _generate_fingerprint(self, df: pd.DataFrame) -> str:
        """Generate a unique fingerprint for the dataset"""
        # Combine key characteristics of the dataset
        characteristics = f"{df.shape}_{df.columns.tolist()}_{df.index[0] if len(df) > 0 else ''}"
        return hashlib.md5(characteristics.encode()).hexdigest()

    def _save_metadata(self, metadata: dict):
        """Save metadata to file"""
        filename = f"metadata_{metadata['dataset_name']}_{metadata['timestamp']}.json"
        filepath = os.path.join(self.metadata_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(metadata, f, indent=4, default=str)

    def _track_lineage(self, dataset_name: str, stage: str, metadata: dict):
        """Track data lineage"""
        lineage = {
            'dataset_name': dataset_name,
            'stage': stage,
            'timestamp': metadata['timestamp'],
            'fingerprint': metadata['data_fingerprint'],
            'row_count': metadata['row_count'],
            'transformations': [],
            'source_datasets': []
        }
        
        filename = f"lineage_{dataset_name}_{metadata['timestamp']}.json"
        filepath = os.path.join(self.lineage_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(lineage, f, indent=4, default=str)

    def log_transformation(self, source_dataset: str, target_dataset: str, 
                         transformation_type: str, transformation_details: dict):
        """Log data transformation details"""
        transformation = {
            'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'source_dataset': source_dataset,
            'target_dataset': target_dataset,
            'transformation_type': transformation_type,
            'transformation_details': transformation_details
        }
        
        filename = f"transformation_{source_dataset}_to_{target_dataset}.json"
        filepath = os.path.join(self.lineage_dir, filename)
        
        existing_transformations = []
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                existing_transformations = json.load(f)
                
        if not isinstance(existing_transformations, list):
            existing_transformations = []
            
        existing_transformations.append(transformation)
        
        with open(filepath, 'w') as f:
            json.dump(existing_transformations, f, indent=4, default=str)