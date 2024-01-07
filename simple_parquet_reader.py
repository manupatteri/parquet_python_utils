#python3 simple_parquet_reader.py example.parquet 
import pyarrow.parquet as pq
import pandas

import sys

print ('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', str(sys.argv))

parquet_table = pq.read_table(sys.argv[1])

with pandas.option_context('display.max_rows', None,):
   print(parquet_table.to_pandas())
