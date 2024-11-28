#Run this:
#spark-submit extractHist.py /courses/datasets/reddit_submissions_repartitioned/year=2020 /courses/datasets/reddit_comments_repartitioned/year=2020 reddit-2020
import sys
from pyspark.sql import SparkSession, functions, types, Row

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

submissions_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('created', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('domain', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.BooleanType()),
    types.StructField('from', types.StringType()),
    types.StructField('from_id', types.StringType()),
    types.StructField('from_kind', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('hide_score', types.BooleanType()),
    types.StructField('id', types.StringType()),
    types.StructField('is_self', types.BooleanType()),
    types.StructField('link_flair_css_class', types.StringType()),
    types.StructField('link_flair_text', types.StringType()),
    types.StructField('media', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('num_comments', types.LongType()),
    types.StructField('over_18', types.BooleanType()),
    types.StructField('permalink', types.StringType()),
    types.StructField('quarantine', types.BooleanType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('saved', types.BooleanType()),
    types.StructField('score', types.LongType()),
    types.StructField('secure_media', types.StringType()),
    types.StructField('selftext', types.StringType()),
    types.StructField('stickied', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('thumbnail', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('url', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])


def main(submissions_path, comments_path, output):
    reddit_submissions_path = submissions_path
    reddit_comments_path = comments_path
    reddit_submissions = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    reddit_comments = spark.read.json(reddit_comments_path, schema=comments_schema)
    subs = ['CanadaPolitics', 'canada', 'canadian', 'onguardforthee', 'canadaleft', 'CanadianConservative']
    subs = list(map(functions.lit, subs))
    reddit_submissions.where(reddit_submissions['subreddit'].isin(subs)) \
        .select('author', 'name', 'created_utc', 'num_comments', "title", 'subreddit', 'subreddit_id', 'score', 'link_flair_css_class',
                                     'link_flair_text', 'author_flair_text', 'year', 'month')\
        .write.parquet(output + '/submissions', mode='overwrite', compression='gzip')
    reddit_comments.where(reddit_comments['subreddit'].isin(subs)) \
        .select('author', 'body', 'created_utc' ,'id', 'link_id',  'parent_id', 'score', 'subreddit', 'subreddit_id', 'year', 'month')\
        .write.parquet(output + '/comments', mode='overwrite', compression='gzip')

    



if __name__ == '__main__':
    submissions_path = sys.argv[1]
    comments_path  = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('reddit extraction').getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    main(submissions_path, comments_path, output)
    
