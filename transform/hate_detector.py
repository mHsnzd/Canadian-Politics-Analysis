'''
This script detects whether hate speech is present in the comments or not using a hugging face pretrained model(Hate-speech-CNERG/bert-base-uncased-hatexplain). 

possible outcomes:
Hatespeech, Offensive, or Normal

To locally run the script use the command:
spark-submit hate_detection_bert.py input-parquet-path output-parquet-path
'''


import sys,os
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row


# Constants
MAX_SEQ_LENGTH = 256        # maximum sequence length for the pretrained model 
COMMENT_TXT_FIELD = 'body' 
HATE_SPEECH_LABEL_FIELD = 'hate_speech'

def main(input_date, dir):
    # Read data from files
    comments_path = f"{dir}/emotion/reddit_comments_{input_date}"
    comments_df = spark.read.parquet(comments_path)

    # Define the pretrained classification model
    hate_detector = pipeline(
        'text-classification',
        model='Hate-speech-CNERG/bert-base-uncased-hatexplain'#'Hate-speech-CNERG/dehatebert-mono-english'
    )

    # Define a udf to apply the classification model on each comment body
    @functions.udf(returnType=types.StringType())
    def detect_hate(comment):
        '''
        Truncate the body of comment to match the max size acceptable by the model
        and then apply the model on it, return whether the comment contains hate speech.
        '''
        truncated_comment = comment[:MAX_SEQ_LENGTH]
        result = hate_detector.predict(truncated_comment)[0]
        return result['label']
    
    # Perform emotion classification
    result_df = comments_df.withColumn(
        HATE_SPEECH_LABEL_FIELD, 
        detect_hate(comments_df[COMMENT_TXT_FIELD])
    ) 

    # result_df.show(vertical=True)
    # Create the transform directory if it doesn't exist
    os.makedirs(f"{dir}/transformed", exist_ok=True)
    # Write the result to a Parquet file
    result_df.coalesce(1).write.option("header", "true").mode("overwrite").parquet(f"{dir}/transformed/reddit_comments_{input_date}")



if __name__ == '__main__':
    spark = SparkSession.builder.appName('hate detector').getOrCreate()
    assert spark.version >= '3.0'  # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    input_date = sys.argv[1]  # Date passed as command-line argument
    data_dir = sys.argv[2]
    main(input_date,data_dir)