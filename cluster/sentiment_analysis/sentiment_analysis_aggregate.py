"""
This script performs basic aggregations on the result of the sentiment analysis. 

To locally run the script, use the command:
spark-submit sentiment_analysis_aggregate.py input-parquet-path output-parquet-path
"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row


# Constants
SENTIMENT_LABEL_FIELD = 'sentiment'
SENTIMENT_SCORE_FIELD = 'sentiment_score'
AVG_GROUP_BY_FIELDS = ['subreddit', 'year', 'month', 'day', 'label']
COUNT_GROUP_BY_FIELDS = ['subreddit', 'year', 'month', 'day', 'label', 'sentiment']
AVG_SCORE_FIELD = 'average_sentiment_score'
COUNT_LABEL_FIELD = 'sentiment_count'


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input).cache()

    # Find the average score and count of labels for each month for each party
    avg_score_result = comments_df \
        .groupby(AVG_GROUP_BY_FIELDS) \
        .agg(
            functions.avg(comments_df[SENTIMENT_SCORE_FIELD]).alias(AVG_SCORE_FIELD)
        ).sort(AVG_GROUP_BY_FIELDS)
    
    count_sentiment_result = comments_df \
        .groupby(COUNT_GROUP_BY_FIELDS) \
        .agg(
            functions.count(comments_df[SENTIMENT_LABEL_FIELD]).alias(COUNT_LABEL_FIELD)
        ).sort(COUNT_GROUP_BY_FIELDS)
    
    # Write the result to a CSV file
    avg_score_result.coalesce(1).write.csv(output+'/average_sentiment_score', mode='overwrite', header=True) 

    count_sentiment_result.coalesce(1).write.csv(output+'/count_sentiment_type', mode='overwrite', header=True)


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.Builder() \
        .appName('Sentiment Analysis Aggregate') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)