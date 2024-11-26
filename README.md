# E-commerce Data Quality Pipeline

## Project Overview
A robust ETL (Extract, Transform, Load) pipeline for processing e-commerce data from the Brazilian E-commerce Public Dataset by Olist. This pipeline includes comprehensive data quality checks and governance features.

## Features
- Data quality validation and monitoring
- Automated data governance tracking
- Sales analysis and metrics calculation
- Customer behavior analysis
- Product performance tracking

## Technical Architecture
- ETL Pipeline Implementation
- Data Quality Framework
- Data Governance System
- Analysis Module

## Setup & Installation
1. Clone the repository:
```bash
git clone https://github.com/anvithakommineni/retail_sales_pipeline.git
```

2. Create virtual environment:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage
Run the pipeline:
```bash
python src/pipeline.py
```

## Project Structure
```
retail_sales_pipeline/
├── src/
│   ├── pipeline.py          # Main ETL pipeline
│   ├── data_quality.py      # Data quality checks
│   ├── data_governance.py   # Data governance tracking
│   ├── config.py           # Configuration settings
│   └── analysis.py         # Analysis module
├── data/
│   ├── raw/                # Input data files
│   └── processed/          # Processed output files
└── logs/                   # Pipeline logs
```

## Data Quality Checks
- Column presence validation
- Null value detection
- Data type verification
- Value range validation
- Duplicate checking

## Data Governance
- Metadata tracking
- Data lineage documentation
- Transformation logging
- Dataset fingerprinting

## Author
Anvitha Kommineni
