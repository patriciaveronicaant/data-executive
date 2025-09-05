from pipeline_config import PipelineConfig
from csv_data_source import CSVDataSource
from datetime import datetime
import pandas as pd
from connection import Connection

def main():

    #Ingestion timestamp - dictionary key-value pair
    result = {
            'success': False,
            'start_time': datetime.now(),
            'end_time': None,
            'rows_processed': 0,
            'errors': []    
        }
    
    config = PipelineConfig(
        source_path= r"C:\Users\Pat\Documents\Pat\train.csv",
        target_connection = 'localhost',
        target_table = 'train',
        batch_size = 1000,
        max_retries = 3,
        data_quality_checks = True
        )
    

    data_source = CSVDataSource(file_path = config.source_path)

    extract = data_source.extract()

    #DATA TRANSFORMATION
    #Standardize column names (lowercase + underscores)
    extract.columns = (
        extract.columns.str.strip()      # remove leading/trailing spaces
                .str.lower()             # lowercase
                .str.replace(r"[\s\-]+", "_", regex=True)  # spaces/dashes -> underscore
    )

    # Rename specific column
    extract = extract.rename(columns={"sales": "sales_amount"})

    # Missing Values - impute
    extract["postal_code"] = extract["postal_code"].fillna(-1)

    #Fix datatype - Convert Data
    extract["order_date"] = pd.to_datetime(extract["order_date"], dayfirst=True, errors="coerce")
    extract["ship_date"] = pd.to_datetime(extract["ship_date"], dayfirst=True, errors="coerce")

    #DATA QUALITY CHECKS
    if config.data_quality_checks:
              
        issues = {}

        # Check missing values
        nulls = extract.isnull().sum()
        result["errors"].append({'missing_values': nulls[nulls > 0].to_dict()})

        # Check duplicates
        duplicate_count = extract.duplicated().sum()
        result["errors"].append({'duplicates': duplicate_count})

        # Remove the 'row_id' column if it exists
        if "row_id" in extract.columns:
            extract = extract.drop(columns=["row_id"])

        # Check for negative sales if column exists
        if "sales_amount" in extract.columns:
            negative_sales = (extract["sales_amount"] < 0).sum()
            result["errors"].append({'negative_sales': int(negative_sales)})

        print("[INFO] Data quality issues found:", issues)

    connection = Connection()
    connection.db_connect()

    #BATCH PROCESSING
    if connection.db:
        cursor = connection.db.cursor()

        # Prepare INSERT query dynamically from DataFrame columns
        cols = ",".join(extract.columns)
        placeholders = ",".join(["%s"] * len(extract.columns))
        insert_sql = f"INSERT INTO {config.target_table} ({cols}) VALUES ({placeholders})"

        # Process in batches
        for start in range(0, len(extract), config.batch_size): #from 0 to 1000
            batch = extract.iloc[start:start + config.batch_size]

            # Convert DataFrame rows into list of tuples
            batch_records = [tuple(row) for row in batch.to_numpy()]

            try:
                cursor.executemany(insert_sql, batch_records)
                #connection.db.commit()
                print(f"[INFO] Inserted batch {start} - {start+len(batch)}")
            except Exception as e:
                #connection.db.rollback()
                print("[ERROR] Failed batch insert:", e)

        cursor.close()
        connection.db.close()

        # result = {
        #     'success': False,
        #     'start_time': datetime.now(),
        #     'end_time': None,
        #     'rows_processed': 0,
        #     'errors': []    
        # }

        result ['success'] = True 
        result ['end_time'] = datetime.now()
        result ['rows_processed'] = len(extract)

        print (result)

    #print (extract)    
    
if __name__ == "__main__":
    main()
   
