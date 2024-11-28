import s3fs
import os, glob
from datetime import datetime, timedelta
import pandas as pd
from utils.constants import *
from etls.reddit_etl import connect_postgres


def create_dataset(dt: datetime):    
    # Calculate the start and end timestamps
    start_timestamp = int((dt - timedelta(days=1)).timestamp())
    end_timestamp = int(dt.timestamp())

    # Create connection to database
    conn = connect_postgres(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT)

    try:
        # Query to retrieve comments within the specified timestamp range
        comments_query = """
        SELECT id, parent_id, author, body, created_utc, score, subreddit, subreddit_id, link_id
        FROM reddit_comments
        WHERE created_utc >= to_timestamp(%s) AND created_utc < to_timestamp(%s)
        """
        
        # Query to retrieve submissions within the specified timestamp range
        submissions_query = """
        SELECT author, id, created_utc, num_comments, subreddit, subreddit_id, title, score, 
               link_flair_template_id, link_flair_text
        FROM reddit_submissions
        WHERE created_utc >= to_timestamp(%s) AND created_utc < to_timestamp(%s)
        """

        # Execute the comments query and fetch data
        with conn.cursor() as cur:
            cur.execute(comments_query, (start_timestamp, end_timestamp))
            comments = cur.fetchall()
            
            # Execute the submissions query and fetch data
            cur.execute(submissions_query, (start_timestamp, end_timestamp))
            submissions = cur.fetchall()

        # Define columns for comments and submissions
        comments_columns = ["id", "parent_id", "author", "body", "created_utc", "score", 
                            "subreddit", "subreddit_id", "link_id"]
        submissions_columns = ["author", "id", "created_utc", "num_comments", "subreddit", 
                               "subreddit_id", "title", "score", "link_flair_template_id", 
                               "link_flair_text"]

        # Convert fetched data to DataFrames for easy manipulation
        comments_df = pd.DataFrame(comments, columns=comments_columns)
        submissions_df = pd.DataFrame(submissions, columns=submissions_columns)

        # Transform the data (assuming transform_data function handles both datasets)
        comments_df = transform_data(comments_df)
        submissions_df = transform_data(submissions_df)

        # Save DataFrames to Parquet files in the output directory
        comments_output_file = os.path.join(OUTPUT_PATH, f"reddit_comments_{(dt - timedelta(days=1)).strftime('%Y%m%d')}.parquet")
        submissions_output_file = os.path.join(OUTPUT_PATH, f"reddit_submissions_{(dt - timedelta(days=1)).strftime('%Y%m%d')}.parquet")

        comments_df.to_parquet(comments_output_file, index=False)
        print(f"Comments dataset saved to {comments_output_file}")
        
        submissions_df.to_parquet(submissions_output_file, index=False)
        print(f"Submissions dataset saved to {submissions_output_file}")

        # # Delete retrieved comments from the database
        # delete_query = """
        # DELETE FROM reddit_comments
        # WHERE created_utc >= to_timestamp(%s) AND created_utc < to_timestamp(%s)
        # """
        
        # with conn.cursor() as cur:
        #     cur.execute(delete_query, (start_timestamp, end_timestamp))
        #     conn.commit()
        # print("Deleted retrieved comments from the database.")

    finally:
        conn.close()

    return comments_output_file, submissions_output_file


def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key= AWS_ACCESS_KEY_ID,
                               secret=AWS_ACCESS_KEY)
        return s3
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/raw/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('The file was not found')

def get_parquet_file(folders):
    files = []
    for folder in folders:
        # Use glob to find all .parquet files in the folder
        parquet_files = glob.glob(os.path.join(folder, "*.parquet"))

        if parquet_files:
            files.append(parquet_files[0])
    return files

def upload_folder_s3(dt: datetime):
    output_folders = [f"{OUTPUT_PATH}/transformed/reddit_comments_{dt.strftime('%Y%m%d')}", 
    f"{OUTPUT_PATH}/transformed/reddit_submissions_{dt.strftime('%Y%m%d')}"]
    output_files = get_parquet_file(output_folders)
    s3 = connect_to_s3()
    for file_path, folder in zip(output_files, output_folders):
        try:
            s3.put(file_path, AWS_BUCKET_NAME+'/transformed/'+ f"{folder.split('/')[-1]}.parquet")
            print('File uploaded to s3')
        except FileNotFoundError:
            print(f'The file {file_path} was not found')


def transform_data(df: pd.DataFrame):
    return df