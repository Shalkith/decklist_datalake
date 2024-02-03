# utility for reading parquet files
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ParquetUtil:
    def __init__(self, parquet_file):
        self.parquet_file = parquet_file

    def read_parquet(self):
        return pd.read_parquet(self.parquet_file)
    
    def split_file(self,rows=500):
        df = pd.read_parquet(self.parquet_file)
        # split the file as many times as needed
        splitfiles = []
        logging.info('Splitting the file into %s row chunks' % rows)
        for i in range(0, len(df), rows):
            logging.info('Writing %s to %s' % (i,i+rows))
            df_part = df[i:i+rows]
            df_part.to_parquet(self.parquet_file+'_part_'+ str(i) + '.parquet')
            splitfiles.append(self.parquet_file+'_part_'+ str(i) + '.parquet')
        return splitfiles
    


if __name__ == '__main__':
    # parquet file to read
    import os
    os.chdir(os.path.dirname(__file__))
    parquet_file = input('Enter the parquet file to read: ')    
    parquet_util = ParquetUtil(parquet_file)
    df = parquet_util.read_parquet()
    print(df.head())
    print(df.tail())
    rows = int(input('Enter the number of rows to split the file by: '))
    parquet_util.split_file(rows)

