"""
This script performs emotion classification on the comments using a Hugging Face pretrained model(j-hartmann/emotion-english-distilroberta-base). 

Out of the 6 possible emotion and the neutral class, we want the most prevalent emotion:
anger, disgust, fear, joy, neutral, sadness, surprise

To locally run the script use the command:
spark-submit emotion_classification_roberta.py input-parquet-path output-parquet-path
"""


import sys,os
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row


# Constants
MAX_SEQ_LENGTH = 256       # maximum sequence length for the pretrained model 
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal']
EMOTION_LABEL_FIELD = 'emotion'
SENTIMENT_SCORE_FIELD = 'sentiment_score'
PARTITION_BY_FIELDS = ['subreddit','year','month','day']


def main(input_date, dir):
    # Read data from files
    comments_path = f"{dir}/sentiment/reddit_comments_{input_date}"
    comments_df = spark.read.parquet(comments_path)

    # Define the pretrained classification model
    emotion_classifier = pipeline(
        'text-classification',
        model='j-hartmann/emotion-english-distilroberta-base', #'SamLowe/roberta-base-go_emotions'
        top_k = 1)

    # Define a udf to apply the classification model on each comment body
    @functions.udf(returnType=types.StringType())
    def classify_emotions(comment):
        '''
        Truncate the body of comment to match the max size acceptable by the model and then apply the model on it, Return the prevalent emotion label
        '''
        truncated_comment = comment[:MAX_SEQ_LENGTH]
        result = emotion_classifier.predict(truncated_comment)[0][0] 
        return result['label']
    
    # Perform emotion classification
    result_df = comments_df.withColumn(
        EMOTION_LABEL_FIELD, 
        classify_emotions(comments_df[COMMENT_TXT_FIELD])
    ) 

    # result_df.show(vertical=True)
    # Create the transform directory if it doesn't exist
    os.makedirs(f"{dir}/emotion", exist_ok=True)
    # Write the result to a Parquet file
    # result.show(truncate=False)
    # Write the result to a Parquet file
    result_df.coalesce(1).write.option("header", "true").mode("overwrite").parquet(f"{dir}/emotion/reddit_comments_{input_date}")

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Emotion Classification RoBERTa').getOrCreate()
    assert spark.version >= '3.0'  # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    input_date = sys.argv[1]  # Date passed as command-line argument
    data_dir = sys.argv[2]
    main(input_date,data_dir)