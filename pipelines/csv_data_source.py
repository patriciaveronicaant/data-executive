import pandas as pd
import os 

class CSVDataSource:
    
    def __init__(self, file_path):
        self.file_path = file_path

    def extract (self):

        # Check if file exists
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        # Check if file is actually a CSV
        if not self.file_path.lower().endswith(".csv"):
            raise ValueError(f"Invalid file type. Expected .csv, got {self.file_path}")
        
        #Extracts data from a CSV file and returns it as a pandas DataFrame.
        df = pd.read_csv(self.file_path)
        return df


        