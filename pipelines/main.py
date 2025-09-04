from pipeline_config import PipelineConfig
from csv_data_source import CSVDataSource
from datetime import datetime
import pandas as pd

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

    #Standardize column names (lowercase + underscores)
    extract.columns = (
        extract.columns.str.strip()      # remove leading/trailing spaces
                .str.lower()             # lowercase
                .str.replace(r"[\s\-]+", "_", regex=True)  # spaces/dashes -> underscore
    )

    # Rename specific column
    extract = extract.rename(columns={"sales": "sales_amount"})

    # Missing Values - impute
    extract["postal_code"].fillna(-1, inplace=True)

    #Fix datatype - Convert Data
    extract["order_date"] = pd.to_datetime(extract["order_date"], dayfirst=True, errors="coerce")
    extract["ship_date"] = pd.to_datetime(extract["ship_date"], dayfirst=True, errors="coerce")


    print (extract)    
    
if __name__ == "__main__":
    main()
   
