import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# Configure logging
log_filename = datetime.now().strftime('logs_%Y-%m-%d.log')
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
logging.info("Starting the code")

class FlipkartETL:


    def __init__(self, bucket_name, file_key, host_name, port_number, db_name, user_name, password):

        self.bucket_name = bucket_name
        self.file_key = file_key
        self.s3_client = boto3.client("s3")

        self.host_name = host_name
        self.port_number = port_number
        self.db_name = db_name
        self.user_name = user_name
        self.password = password
        
        
        os.makedirs("artifacts",exist_ok=True)

    
    def extract_data(self):

        logging.info(f"Extracting data from S3 bucket: {self.bucket_name}, file: {self.file_key}")

        csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)

        raw_data = pd.read_csv(csv_obj["Body"])
        
        logging.info("Data extraction completed")
        
        # Save raw data to artifacts folder
        raw_data_path = os.path.join('artifacts', 'raw_data.csv')
        
        raw_data.to_csv(raw_data_path, index=False)
        
        logging.info(f"Raw data saved to {raw_data_path}")
        
        return raw_data

    
    def transform_data(self, raw_data):
        logging.info("Starting data transformation")
        
        # Drop unnecessary columns
        prefix = "Unnamed:"
        cols_to_drop = raw_data.filter(like=prefix).columns
        raw_data.drop(columns=cols_to_drop, inplace=True)
        
        # Extract Brand Name from Title
        raw_data['Brand_Name'] = raw_data['Title'].str.split().str[0]
        raw_data.drop("Title", inplace=True, axis=1)
        
        # Transform the Processor column
        raw_data['Processor'] = raw_data['Processor'].str.split('(').str[0].str.strip()
        raw_data['Processor'] = raw_data['Processor'].str.replace('Intel Core 5 Processor', 'Intel Core i5 Processor')
        raw_data['Processor'] = raw_data['Processor'].str.replace('Intel Core 7 Processor', 'Intel Core i7 Processor')

        # Transform the RAM column
        raw_data['RAM'] = raw_data['RAM'].str.split().str[:2].apply(' '.join)
        
        # Transform the Operating System column
        raw_data['Operating_System'] = raw_data['Operating_System'].str.replace(
            '64 bit Windows 11 Home Operating System', '64 bit Windows 11 Operating System')
        
        # Transform the Warranty column
        raw_data['Warranty'] = raw_data['Warranty'].str.split().str[:2].apply(' '.join)
        warranty_replacements = {
            '1 Yr': '1 Year', '1 Yera': '1 Year', '1 Years ': '1 Year',
            '1 year': '1 Year', '1 Years': '1 Year', 'One-year International': '1 Year',
            '3 Years': '3 Year', '2 Years': '2 Year'
        }
        for key, value in warranty_replacements.items():
            raw_data['Warranty'] = raw_data['Warranty'].str.replace(key, value)

        # Transform the Prices column
        raw_data['Prices'] = raw_data['Prices'].str.replace(',', '').str.replace(r'\D', '', regex=True)
        raw_data['Prices'] = pd.to_numeric(raw_data['Prices'], errors='coerce').astype('Int64', errors='ignore')

        # Transform the Rating column
        rating_replacements = {'74,990': '4.3', '57,499': '4.3'}
        for key, value in rating_replacements.items():
            raw_data['Rating'] = raw_data['Rating'].str.replace(key, value, regex=True)

        logging.info("Data transformation completed")
        
        # Save transformed data to artifacts folder
        transformed_data_path = os.path.join('artifacts', 'transformed_data.csv')
        raw_data.to_csv(transformed_data_path, index=False)
        logging.info(f"Transformed data saved to {transformed_data_path}")
        
        return raw_data.head(50)

    
    def load_data_to_redshift(self, transformed_data, table_name):

        logging.info(f"Loading data into Redshift table: {table_name}")
        
        conn_str = (
            f"postgresql+psycopg2://{self.user_name}:{self.password}"
            f"@{self.host_name}:{self.port_number}/{self.db_name}"
        )
        
        engine = create_engine(conn_str)
        
        transformed_data.to_sql(table_name, engine, index=False, if_exists='replace')
        
        logging.info("Data loaded successfully into Redshift")

    
    def run_etl(self, table_name):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data_to_redshift(transformed_data, table_name)


if __name__ == "__main__":

    etl = FlipkartETL(
        bucket_name="flipkartdata2521",
    
        file_key="Flipkart_data.csv",
    
        host_name="redshift-cluster-2.czcdbl82hrrx.us-east-1.redshift.amazonaws.com",
    
        port_number="5439",
    
        db_name="my_db",
    
        user_name="awsuser",
    
        password="Karthiksara2123"
    )
    
    etl.run_etl(table_name="flipkart_data")