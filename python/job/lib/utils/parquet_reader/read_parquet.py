# utility for reading parquet files
import pandas as pd

class ParquetUtil:
    def __init__(self, parquet_file):
        self.parquet_file = parquet_file

    def read_parquet(self):
        return pd.read_parquet(self.parquet_file)
    


if __name__ == '__main__':
    # parquet file to read
    import os
    os.chdir(os.path.dirname(__file__))
    parquet_file = input('Enter the parquet file to read: ')    
    parquet_util = ParquetUtil(parquet_file)
    df = parquet_util.read_parquet()
    print(df.head())
    print(df.tail())
