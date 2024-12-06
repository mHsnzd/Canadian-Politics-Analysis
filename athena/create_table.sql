CREATE EXTERNAL TABLE reddit.nlp (
    id STRING,
    label STRING,
    parent_id STRING,
    author STRING,
    created_utc DOUBLE,
    score INT,
    subreddit STRING,
    subreddit_id STRING,
    link_id STRING,
    datetime BIGINT,
    year INT,
    month INT,
    day INT,
    body STRING,
    sentiment STRING,
    sentiment_score DOUBLE,
    emotion STRING,
    hate_speech STRING
)
STORED AS PARQUET
LOCATION 's3://cmpt732/transformed/';



