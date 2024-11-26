import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import logging

class SalesAnalyzer:
    def __init__(self):
        """Initialize the sales analyzer"""
        self.processed_dir = '../data/processed'
        self.analysis_dir = '../data/analysis'
        os.makedirs(self.analysis_dir, exist_ok=True)
        
        # Set up Seaborn style
        sns.set_style("whitegrid")
        plt.style.use('seaborn')

    def load_latest_data(self):
        """Load the most recent processed sales data"""
        # Get the latest file
        files = os.listdir(self.processed_dir)
        sales_files = [f for f in files if f.startswith('processed_sales_')]
        latest_file = max(sales_files)
        
        # Load the data
        df = pd.read_csv(os.path.join(self.processed_dir, latest_file))
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        return df

    def generate_daily_sales_report(self, df):
        """Generate daily sales analysis"""
        # Group by date
        daily_sales = df.groupby(df['transaction_date'].dt.date).agg({
            'final_amount': 'sum',
            'transaction_id': 'count',
            'quantity': 'sum'
        }).reset_index()
        
        daily_sales.columns = ['date', 'total_sales', 'num_transactions', 'items_sold']
        return daily_sales

    def plot_daily_sales_trend(self, df):
        """Plot daily sales trend"""
        daily_sales = self.generate_daily_sales_report(df)
        
        plt.figure(figsize=(12, 6))
        plt.plot(daily_sales['date'], daily_sales['total_sales'], marker='o')
        plt.title('Daily Sales Trend')
        plt.xlabel('Date')
        plt.ylabel('Total Sales ($)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save plot
        plt.savefig(os.path.join(self.analysis_dir, 'daily_sales_trend.png'))
        plt.close()

    def plot_hourly_sales_pattern(self, df):
        """Plot hourly sales pattern"""
        hourly_sales = df.groupby('hour')['final_amount'].mean().reset_index()
        
        plt.figure(figsize=(10, 6))
        sns.barplot(x='hour', y='final_amount', data=hourly_sales)
        plt.title('Average Sales by Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Average Sales ($)')
        plt.tight_layout()
        
        plt.savefig(os.path.join(self.analysis_dir, 'hourly_sales_pattern.png'))
        plt.close()

    def plot_store_performance(self, df):
        """Plot store performance comparison"""
        store_sales = df.groupby('store_id').agg({
            'final_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Total sales by store
        sns.barplot(x='store_id', y='final_amount', data=store_sales, ax=ax1)
        ax1.set_title('Total Sales by Store')
        ax1.set_xlabel('Store ID')
        ax1.set_ylabel('Total Sales ($)')
        
        # Transaction count by store
        sns.barplot(x='store_id', y='transaction_id', data=store_sales, ax=ax2)
        ax2.set_title('Transaction Count by Store')
        ax2.set_xlabel('Store ID')
        ax2.set_ylabel('Number of Transactions')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.analysis_dir, 'store_performance.png'))
        plt.close()

    def generate_summary_report(self, df):
        """Generate summary statistics report"""
        summary = {
            'total_sales': df['final_amount'].sum(),
            'total_transactions': len(df),
            'average_transaction': df['final_amount'].mean(),
            'total_items_sold': df['quantity'].sum(),
            'average_items_per_transaction': df['quantity'].mean(),
            'most_active_store': df.groupby('store_id')['transaction_id'].count().idxmax(),
            'best_performing_store': df.groupby('store_id')['final_amount'].sum().idxmax(),
            'peak_hour': df.groupby('hour')['final_amount'].sum().idxmax()
        }
        
        # Save summary report
        with open(os.path.join(self.analysis_dir, 'summary_report.txt'), 'w') as f:
            f.write("=== Sales Analysis Summary ===\n\n")
            for key, value in summary.items():
                if isinstance(value, float):
                    f.write(f"{key.replace('_', ' ').title()}: ${value:,.2f}\n")
                else:
                    f.write(f"{key.replace('_', ' ').title()}: {value:,}\n")

    def run_analysis(self):
        """Run all analyses"""
        try:
            print("Starting sales analysis...")
            
            # Load data
            df = self.load_latest_data()
            
            # Generate all plots
            self.plot_daily_sales_trend(df)
            self.plot_hourly_sales_pattern(df)
            self.plot_store_performance(df)
            
            # Generate summary report
            self.generate_summary_report(df)
            
            print("Analysis completed successfully!")
            print(f"Results saved in: {self.analysis_dir}")
            
        except Exception as e:
            print(f"Error during analysis: {str(e)}")
            raise

if __name__ == "__main__":
    analyzer = SalesAnalyzer()
    analyzer.run_analysis()