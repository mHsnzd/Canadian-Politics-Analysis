import pandas as pd 


df = pd.read_parquet('reddit_submissions_all.parquet')

print(len(df))