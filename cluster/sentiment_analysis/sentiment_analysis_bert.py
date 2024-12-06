"""
This script performs sentiment analysis on the party-labeled comments using a Hugging Face pretrained model(distilbert). 

This model outputs a POSITIVE/NEGATIVE sentiment and its score.

To locally run the script use the command:
spark-submit sentiment_analysis_bert.py input-parquet-path output-parquet-path
"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row


# Constants
MAX_SEQ_LENGTH = 512        # maximum sequence length for the pretrained model 
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal']
SENTIMENT_LABEL_FIELD = 'sentiment'
SENTIMENT_SCORE_FIELD = 'sentiment_score'
PARTITION_BY_FIELDS = ['subreddit','year','month','day']


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)
    # # Ignore neither or both labels
    # comments_df = comments_df.filter(comments_df[PARTY_LABEL_FIELD].isin(PARTY_LABELS))

    # Define and use the pretrained model
    model = pipeline(
        'sentiment-analysis',
        model='distilbert-base-uncased-finetuned-sst-2-english')

    # Define a udf to apply the sentiment model on each comment body
    @functions.udf(returnType=types.StructType([ 
        types.StructField('label', types.StringType(), True), 
        types.StructField('score', types.FloatType(), True) 
    ]))
    def analyze_sentiment(comment):
        '''
        Truncate the body of comment to match the max size acceptable by the model and then apply the model on it, Return sentiment and its score
        '''
        truncated_comment = comment[:MAX_SEQ_LENGTH]
        result = model.predict(truncated_comment)[0] 
        return Row(label=result['label'], score=result['score'])
    
    # Perform sentiment analysis
    analyzed_comments_df = comments_df.withColumn(
        'sentiment_analysis', 
        analyze_sentiment(comments_df[COMMENT_TXT_FIELD])
    ) 

    result_df = analyzed_comments_df.withColumns({
        SENTIMENT_LABEL_FIELD: analyzed_comments_df['sentiment_analysis.label'],
        SENTIMENT_SCORE_FIELD: 
            functions.when(
                analyzed_comments_df['sentiment_analysis.label']==functions.lit('POSITIVE'), 
                functions.lit(2) * analyzed_comments_df['sentiment_analysis.score'] - functions.lit(1)
            ).when(
                analyzed_comments_df['sentiment_analysis.label']==functions.lit('NEGATIVE'),
                functions.lit(1) - functions.lit(2) * analyzed_comments_df['sentiment_analysis.score']
            ).otherwise(0)
    }).drop('sentiment_analysis')

    # Write the result to a Parquet file
    # result.show(truncate=False)
    result_df.write\
        .partitionBy(PARTITION_BY_FIELDS)\
        .parquet(output, mode='overwrite')  


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.Builder() \
        .appName('Sentiment Analysis Bert') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)