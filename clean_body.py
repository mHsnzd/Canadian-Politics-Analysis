import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

comments_schema = types.StructType([
    types.StructField('author', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('score', types.LongType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

# add more functions as necessary

def main(comments_path, output):

    reddit_comments_path = comments_path

    df = spark.read.parquet(reddit_comments_path, schema=comments_schema)

    df_cleaned = df.withColumn('body', functions.regexp_replace(functions.col('body'), r'[\n\r\t]', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'http[s]?://\S+', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'www\.\S+', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'[\\/]', ''))

    df_cleaned = df_cleaned.filter(functions.col('body').isNotNull() & (functions.length(functions.col('body')) > 0) & \
    (~functions.lower(functions.col('body')).isin('[deleted]', '[removed]')))

    df_cleaned.write.parquet(output, mode='overwrite', compression='gzip')

    # main logic starts here

if __name__ == '__main__':
    comments_path = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('clean').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(comments_path, output)