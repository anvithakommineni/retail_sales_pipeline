# E-commerce Data Quality Pipeline

## Project Overview
This project implements a robust ETL (Extract, Transform, Load) pipeline for e-commerce data with comprehensive data quality checks and governance. Built using Python, it processes the Brazilian E-commerce Public Dataset by Olist.

## Features
- Data Quality Validation
  - Column presence validation
  - Null value detection
  - Data type verification
  - Value range validation
  - Duplicate checking
  
- Data Governance
  - Metadata tracking
  - Data lineage documentation
  - Transformation logging
  - Dataset fingerprinting

## Project Structure
```
retail_sales_pipeline/
├── data/
│   ├── raw/                # Raw data files
│   └── processed/          # Processed output files
├── src/
│   ├── pipeline.py         # Main ETL pipeline
│   ├── data_quality.py     # Data quality checks
│   ├── data_governance.py  # Data governance tracking
│   └── config.py          # Configuration settings
├── logs/                   # Quality reports and logs
└── requirements.txt        # Project dependencies
```

## Setup
1. Clone the repository:
```bash
git clone [your-repo-url]
cd retail_sales_pipeline
```

2. Create virtual environment:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place the CSV files in the `data/raw` directory.

## Usage
Run the pipeline:
```bash
cd src
python pipeline.py
```

## Reports
The pipeline generates:
- Data quality reports in `logs/data_quality_reports/`
- Metadata tracking in `logs/metadata/`
- Data lineage in `logs/lineage/`

## Technologies Used
- Python
- Pandas
- NumPy
- Logging