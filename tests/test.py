from datetime import datetime, timedelta
import praw
import psycopg2

dt = datetime(2024,11,10)
# Calculate the timestamp for 24 hours ago
start_timestamp = int((dt - timedelta(days=1)).timestamp())

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

# Query posts from the reddit_submissions table created within the last 24 hours
with conn.cursor() as cur:
    cur.execute("""
        SELECT id FROM reddit_submissions
        WHERE created_utc >= %s
    """, (datetime.fromtimestamp(start_timestamp),))
    
    post_ids = [row[0] for row in cur.fetchall()]

# Process each post ID to retrieve comments
for post_id in post_ids:
    submission = reddit_instance.submission(id=post_id)
    
    # Ensure all comments are fully loaded
    submission.comments.replace_more(limit=None)

    # Extract each comment and add it to the comments_data list
    for comment in submission.comments.list():
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