import pandas as pd
import pyarrow.parquet as pq

parquet_file = "DiscountImpactOutput.parquet"
df = pd.read_parquet(parquet_file)

print(df)
