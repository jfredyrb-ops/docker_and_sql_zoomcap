import sys
import pandas as pd


print("arguments", sys.argv)
day = int(sys.argv[1]) if len(sys.argv) > 1 else 1

df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})

print(df.head())

df.to_parquet(f"output_day_{day}.parquet")

print(f"Running pipeline for day {day}")


print("Pipeline module loaded successfully.")
print(f"Python version: {sys.version}")
