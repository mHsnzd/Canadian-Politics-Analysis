"""
This script performs sentiment analysis on the labeled comments using Bert. 

To locally run the script use the command:
spark-submit sentiment_analysis_spark.py input-parquet-path output-parquet-path

"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, functions, types


# Constants
MAX_SEQ_LENGTH = 512        # maximum sequence length for the pretrained model 
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal'] 
SENTIMENT_LABEL_FIELD = 'sentiment'


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)
    # Ignore neither or both labels
    comments_df = comments_df.filter(comments_df[PARTY_LABEL_FIELD].isin(PARTY_LABELS))

    # Define and use the pretrained model
    model = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

    # Define a udf to implement the model on each comment body
    @functions.udf(returnType=types.StringType())
    def analyze_sentiment(comment):
        '''Truncate the body of comment to match the max size acceptable by the model and then apply the model on it'''
        truncated_comment = comment[:MAX_SEQ_LENGTH]
        result = model.predict(truncated_comment)[0]
        
        return result['label']
    
    comments_df = comments_df.withColumn(SENTIMENT_LABEL_FIELD, analyze_sentiment(comments_df[COMMENT_TXT_FIELD]))

    # Write the result to a Parquet file
    # comments_df.show(truncate=False)
    comments_df.write.parquet(output, mode='overwrite')


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