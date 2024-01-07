#python3 simple_parquet_reader.py example.parquet 
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

print ('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', str(sys.argv))

#df = pd.DataFrame({'one': [1, 2, 3],
#                   'two': [1.0, 2.0, 3.0]})
#                   #index=list('abc'))
#table = pa.Table.from_pandas(df)

arr_float_1 = pa.array(list(map(float, range(100))))
arr_float_2 = pa.array(list(map(float, range(200, 300, 1))))
data_float = [arr_float_1, arr_float_2]
table = pa.Table.from_arrays(data_float, names=['columnA', 'columnB'])

writer = pq.ParquetWriter(sys.argv[1], schema=table.schema, use_dictionary=False,compression="gzip", use_byte_stream_split=True)
#writer = pq.ParquetWriter(sys.argv[1], schema=table.schema, use_dictionary=False,compression="gzip")
writer.write_table(table)
#Since we are dealing with raw APIs, things like closing file object is left upon us. 
writer.close()

