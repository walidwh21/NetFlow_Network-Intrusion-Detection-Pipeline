"""Print the columns and first rows of a flow CSV.   python scripts/inspect_csv.py [path]"""
import sys

import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "data/sample/netflow_sample.csv"
df = pd.read_csv(path, nrows=1000, low_memory=False)
print(f"{path}: {len(df):,} rows (sampled), {len(df.columns)} columns\n")
for i, name in enumerate(df.columns, 1):
    print(f"{i:3d}. {name}")
print("\n", df.head())
