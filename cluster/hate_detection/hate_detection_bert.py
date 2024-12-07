'''
This script detects whether hate speech is present in the comments or not using a hugging face pretrained model(Hate-speech-CNERG/bert-base-uncased-hatexplain). 

possible outcomes:
Hatespeech, Offensive, or Normal

To locally run the script use the command:
spark-submit hate_detection_bert.py input-parquet-path output-parquet-path
'''


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row


# Constants
MAX_SEQ_LENGTH = 256        # maximum sequence length for the pretrained model 
COMMENT_TXT_FIELD = 'body' 
PARTY_LABELS = ['conservative', 'liberal']
HATE_SPEECH_LABEL_FIELD = 'hate_speech'
PARTITION_BY_FIELDS = ['subreddit','year','month','day']

def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)

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

    # Write the result to a Parquet file
    result_df.write\
        .partitionBy(PARTITION_BY_FIELDS)\
        .parquet(output, mode='overwrite')  


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.Builder() \
        .appName('Hate Detection BERT') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)
