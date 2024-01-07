import pyarrow.parquet as pq

parquet_table = pq.read_table('example.parquet')

print(parquet_table.to_pandas())


