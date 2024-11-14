from datetime import datetime, timedelta
import praw
import psycopg2
import pandas as pd

reddit_instance = praw.Reddit(
            client_id='vVh4jQrGTCBh-M_mF6sz3w',
            client_secret='O49Qwt2RC01Kt6bAhVEz68py7Wv4ag',
            user_agent='Reddit Agent',
            username='ducanh07',
            password='Doducanh2707@'
        )

conn = psycopg2.connect(database = 'airflow_reddit',
                    user = 'postgres',
                    password = 'postgres',
                    host = 'localhost',
                    port = 5432)


submissions_file = f'reddit_submissions_all.parquet'
comments_file = f'reddit_comments_all.parquet'

# Query and save data from reddit_submissions table to a Parquet file
query_submissions = """
    SELECT * FROM reddit_submissions
"""
df_submissions = pd.read_sql(query_submissions, conn)
df_submissions.to_parquet(submissions_file, engine='pyarrow', index=False)
print(f"Data from reddit_submissions saved to {submissions_file}")

# Query and save data from reddit_comments table to a Parquet file
query_comments = """
    SELECT * FROM reddit_comments
"""
df_comments = pd.read_sql(query_comments, conn)
df_comments.to_parquet(comments_file, engine='pyarrow', index=False)
print(f"Data from reddit_comments saved to {comments_file}")

# Close the connection
conn.close()
