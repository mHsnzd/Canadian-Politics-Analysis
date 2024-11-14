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
        CREATE TABLE IF NOT EXISTS reddit_submissions (
            author VARCHAR,  
            id VARCHAR PRIMARY KEY,  
            created_utc TIMESTAMP, 
            num_comments INT,  
            subreddit VARCHAR, 
            subreddit_id VARCHAR, 
            title TEXT,
            score INT, 
            link_flair_template_id VARCHAR,  
            link_flair_text VARCHAR
        );
    """

    # Execute the query to create the table
    cur.execute(create_table_query)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS reddit_comments (
        id TEXT PRIMARY KEY,
        parent_id TEXT,
        author TEXT,
        body TEXT,
        created_utc TIMESTAMP,
        score INTEGER,
        subreddit TEXT,
        subreddit_id TEXT,
        link_id TEXT
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
            cur.execute("SELECT 1 FROM reddit_submissions WHERE id = %s", ('t3_'+post.id,))
            if cur.fetchone():
                continue  # Skip this post if it already exists
            
            # Insert post details into the database
            insert_query = """
            INSERT INTO reddit_submissions (id, author, created_utc, num_comments, subreddit, subreddit_id, title, score, link_flair_template_id, link_flair_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            post_data = (
                't3_' + post.id,
                post.author.name if post.author else None,
                datetime.fromtimestamp(post.created_utc),
                post.num_comments,
                subreddit,
                post.subreddit_id,
                post.title,
                post.score,
                post.link_flair_template_id if hasattr(post, 'link_flair_template_id') else None,
                post.link_flair_text if hasattr(post, 'link_flair_text') else None
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
                id, parent_id, author, body, created_utc, score, subreddit, link_id, subreddit_id
            )
            VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """

            # Prepare the comment data as a tuple
            comment_data = (
                comment.id,
                comment.parent_id,
                comment.author.name if comment.author else None,
                comment.body,
                comment.created_utc,
                comment.score,
                comment.subreddit.display_name,
                comment.link_id,
                comment.subreddit_id  # Adding subreddit_id to match the new table structure
            )

            # Execute the insert query
            cur.execute(insert_query, comment_data)
            conn.commit()
            comments += 1
    return comments
