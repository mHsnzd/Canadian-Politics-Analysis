import sys

import numpy as np
import pandas as pd
import praw
import time
from datetime import datetime, timedelta
from tqdm import tqdm

from utils.constants import *

import psycopg2

def connect_reddit(client_id, client_secret, username, password) -> praw.Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent='Reddit Agent',
            username=username,
            password=password
        )
        print("Connected to Reddit successfully!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)

def connect_postgres(db_name, db_user, db_password, db_host, db_port):
    try:
        conn = psycopg2.connect(database = db_name,
                            user = db_user,
                            password = db_password,
                            host = db_host,
                            port = db_port)
        print("Connected to Postgres database successfully!")
        create_table_script(conn)
        return conn
    except Exception as e:
        print(e)
        sys.exit(1)  

def create_table_script(conn: psycopg2.extensions.connection ):
    # Create a cursor object to interact with the database
    cur = conn.cursor()

    # Define the SQL query to create the reddit_submissions table
    create_table_query = """
        CREATE TABLE reddit_submissions (
            created_utc TIMESTAMP,
            id TEXT PRIMARY KEY,
            num_comments INT,
            score INT,
            subreddit TEXT,
            subreddit_id TEXT,
            title TEXT,
            ups INT,
            downs INT,
            year INT,
            month INT
        );  
    """

    # Execute the query to create the table
    cur.execute(create_table_query)

    create_table_query = """
        CREATE TABLE reddit_comments (
            body TEXT,
            controversiality INT,
            created_utc TIMESTAMP,
            downs INT,
            id TEXT PRIMARY KEY,
            link_id TEXT,
            parent_id TEXT,
            score INT,
            subreddit TEXT,
            subreddit_id TEXT,
            ups INT,
            author TEXT,
            year INT,
            month INT,
            author_flair_text TEXT
        );
    """
    # Execute the create table query
    cur.execute(create_table_query)

    # Commit the transaction
    conn.commit()
    # Close the cursor and connection
    cur.close()

    print("Table reddit_comments and reddit_submissions created successfully.")
    
def extract_submissions(reddit_instance: praw.Reddit, conn: psycopg2.extensions.connection, subreddit: str, limit=None):
    subreddit_instance = reddit_instance.subreddit(subreddit)
    submissions = 0
    # Retrieve posts using subreddit.new() to get the latest posts
    for post in subreddit_instance.new(limit=limit):     
        # Check if the post ID already exists in the database
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM reddit_submissions WHERE id = %s", (post.id,))
            if cur.fetchone():
                continue  # Skip this post if it already exists
            
            # Insert post details immediately into the database
            insert_query = """
            INSERT INTO reddit_submissions (id, subreddit, created_utc, downs, author, score, ups, title, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            post_data = (
                post.id,
                subreddit,
                datetime.fromtimestamp(post.created_utc),
                post.downs,
                post.author.name if post.author else None,
                post.score,
                post.ups,
                post.title,
                post.url
            )
            cur.execute(insert_query, post_data)
            conn.commit()
            submissions += 1
    return submissions

# Function to extract comments and insert to reddit_comments table
def extract_comments(reddit_instance: praw.Reddit, conn: psycopg2.extensions.connection, subreddit: str, limit = None):
    # Get subreddit instance
    subreddit_instance = reddit_instance.subreddit(subreddit)
    comments = 0
    for comment in subreddit_instance.comments(limit=limit):
        with conn.cursor() as cur:
            # Check if the comment already exists in the database
            cur.execute("SELECT 1 FROM reddit_comments WHERE id = %s", (comment.id,))
            if cur.fetchone():
                continue  # Skip this comment if it already exists
            
            # Prepare insert query for reddit_comments table
            insert_query = """
            INSERT INTO reddit_comments (
                id, parent_id, parent_type, author, body, created_utc, score, subreddit,
                controversiality, downs, ups
            )
            VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            
            # Prepare the comment data as a tuple
            comment_data = (
                comment.id,
                comment.parent_id.split('_')[1],
                comment.parent_id.split('_')[0],
                comment.author.name if comment.author else None,
                comment.body,
                comment.created_utc,
                comment.score,
                comment.subreddit.display_name,
                comment.controversiality,
                comment.downs,
                comment.ups
            )

            # Execute the insert query
            cur.execute(insert_query, comment_data)
            conn.commit()
            comments += 1
    return comments
