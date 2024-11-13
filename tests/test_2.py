import pandas as pd 


df = pd.read_parquet('reddit_comments_all.parquet')

print(df['author'])