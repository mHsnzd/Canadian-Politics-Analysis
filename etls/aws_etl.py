import s3fs
import os 
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
        query = """
        SELECT *
        FROM 
            reddit_comments
        WHERE 
            created_utc >= to_timestamp(%s) AND created_utc < to_timestamp(%s)
        """

        # Execute the query and fetch data
        with conn.cursor() as cur:
            cur.execute(query, (start_timestamp, end_timestamp))
            comments = cur.fetchall()

        # Convert to DataFrame for easy manipulation
        columns = ["id", "parent_id", "parent_type", "author", "body", "created_utc", "score", 
                   "subreddit", "controversiality", "downs", "ups"]
        comments_df = pd.DataFrame(comments, columns=columns)

        # Transform the data
        comments_df = transform_data(comments_df)

        # Save DataFrame to a Parquet file in the output directory
        output_file = os.path.join(OUTPUT_PATH, f"reddit_comments_{(dt - timedelta(days=1)).strftime('%Y%m%d')}.parquet")
         
        if len(comments) > 0:
            comments_df.to_parquet(output_file, index=False)
            print(f"Dataset saved to {output_file}")

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

    return output_file


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


def transform_data(df: pd.DataFrame):
    return df