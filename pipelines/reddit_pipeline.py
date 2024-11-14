import pandas as pd
from datetime import datetime

from etls.reddit_etl import *
from utils.constants import *

def reddit_pipeline(subreddit:str, limit=None):
    # Connecting to Reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, USERNAME, PASSWORD)
    # Connecting to Postgres database
    db_connection = connect_postgres(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT)

    print(f'Crawling submissions and comments for {subreddit}')
    # Insert new posts to database
    submissions = extract_submissions(instance, db_connection, subreddit, limit)
    # Insert new comments to database
    comments = extract_comments(instance,db_connection, subreddit, limit)
    print(f'Inserted {submissions} new submissions and {comments} new comments to database!')
