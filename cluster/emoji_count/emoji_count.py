"""
This script counts the number of emojis used in the text of comments. 

To locally run the script use the command:
spark-submit emoji_count.py input-parquet-path output-parquet-path
"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row
import re


# Constants
COMMENT_TXT_FIELD = 'body' 
EMOJI_FIELD = 'emoji'
EMOJI_COUNT_FIELD = 'emoji_count'
EMOJI_GROUP_BY_FIELDS = ['subreddit', 'year', 'month', 'day', 'label', 'sentiment', 'emotion', 'hate_speech']


@functions.udf(returnType=types.ArrayType(types.StringType()))
def extract_emojis(comment):
    '''
    Extract emojis and symbols from each comment and
    return an array of the result
    '''
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                            #    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                            #    u"\U0001F1E6-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"  # Dingbats
                            #    u"\U000024C2-\U0001F251"
                               "]", flags=re.UNICODE)
    return emoji_pattern.findall(comment)


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input) \
        .select(*(EMOJI_GROUP_BY_FIELDS + [COMMENT_TXT_FIELD]))

    # Find the emojis used 
    emoji_df = comments_df.select(
        *EMOJI_GROUP_BY_FIELDS,
        functions.explode(
            extract_emojis(comments_df[COMMENT_TXT_FIELD])
        ).alias(EMOJI_FIELD),
        functions.lit(1).alias('temp_count')
    )
    
    # Find the count of emojis for the group by fields
    result_df = emoji_df \
        .groupby(*([EMOJI_FIELD]+EMOJI_GROUP_BY_FIELDS)) \
        .agg(
            functions.sum(emoji_df['temp_count']).alias(EMOJI_COUNT_FIELD)
        ).sort(*([EMOJI_FIELD]+EMOJI_GROUP_BY_FIELDS))
    
    # Write the result to a CSV file
    result_df.coalesce(1).write.csv(output, mode='overwrite', header=True) 


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]       

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.Builder() \
        .appName('Emoji Count') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)