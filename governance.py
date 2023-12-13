import time
import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data(data):
    """Perform data validation checks."""
    if data.isnull().values.any():
        raise ValueError("Data contains null values.")
    # Add more validation rules as needed

def extract(file_path):
    """Extract data from a CSV file."""
    try:
        data = pd.read_csv(file_path)
        validate_data(data)  # Data validation
        logging.info(f"Data extracted successfully from {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error in extract: {e}")
        raise

def transform(new_data, previous_data=None):
    """Transform data by calculating total sales and handling CDC."""
    try:
        new_data['TotalSales'] = new_data['Price'] * new_data['Quantity']

        if previous_data is not None and not previous_data.empty:
            combined_data = pd.concat([previous_data, new_data])
            combined_data = combined_data.drop_duplicates(subset=['Date', 'ProductID'], keep='last')
        else:
            combined_data = new_data

        logging.info("Data transformation completed.")
        return combined_data.groupby('ProductID')['TotalSales'].sum().reset_index()
    except Exception as e:
        logging.error(f"Error in transform: {e}")
        raise

def audit_log(action, details):
    """Function to log audit trail."""
    logging.info(f"Audit Log: {action} - {details}")

def load(data, output_file_path):
    """Load data into a new CSV file."""
    try:
        data.to_csv(output_file_path, index=False)
        audit_log("Data Load", f"Data loaded into {output_file_path}")
        logging.info(f"Data loaded into {output_file_path}")
    except Exception as e:
        logging.error(f"Error in load: {e}")
        raise

def get_previous_data(output_file_path):
    """Get previously processed data if available."""
    try:
        if os.path.exists(output_file_path):
            logging.info(f"Fetching data from {output_file_path}")
            return pd.read_csv(output_file_path)
        else:
            logging.info("No previous data found.")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error in get_previous_data: {e}")
        raise

def run_pipeline(input_file_path, output_file_path):
    """Run the data pipeline."""
    start_time = datetime.now()
    try:
        audit_log("Pipeline Start", f"Started at {start_time}")

        previous_data = get_previous_data(output_file_path)
        new_data = extract(input_file_path)
        transformed_data = transform(new_data, previous_data)
        load(transformed_data, output_file_path)

        end_time = datetime.now()
        audit_log("Pipeline End", f"Ended at {end_time} - Duration: {end_time - start_time}")
        logging.info("Pipeline executed successfully.")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == '__main__':
    run_pipeline('/Users/sandeepdiddi/Library/CloudStorage/OneDrive-Personal/Fractal/Clean_Coding/datapipeline/input_data.csv', '/Users/sandeepdiddi/Library/CloudStorage/OneDrive-Personal/Fractal/Clean_Coding/datapipeline/output_gov.csv')
