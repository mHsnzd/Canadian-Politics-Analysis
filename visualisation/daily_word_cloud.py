'''
This script creates a word cloud image for the comments of each day.

To locally run the script use the command:
spark-submit daily_word_cloud.py input-parquet-path output-png-path
'''


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, regexp_replace


# Constants 
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
CONSERVATIVE_LABEL = 'conservative'
LIBERAL_LABEL = 'liberal'


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input).select(COMMENT_TXT_FIELD, PARTY_LABEL_FIELD)

    # Quick preprocessing for the word cloud
    comments_df = comments_df.withColumn(
        COMMENT_TXT_FIELD,
        functions.regexp_replace(
            comments_df[COMMENT_TXT_FIELD], '[^a-zA-Z\\s]', ''
        )
    )

    # Define a model to remove stop words
    tokenizer = Tokenizer(inputCol=COMMENT_TXT_FIELD, outputCol='tokens')

    remover = StopWordsRemover(inputCol='tokens', outputCol='filtered_tokens')

    pipeline = Pipeline(stages=[tokenizer, remover])
    
    # Remove the stop words
    model = pipeline.fit(comments_df)
    comments_df = model.transform(comments_df).drop('tokens',COMMENT_TXT_FIELD).cache()

    # Since the daily data is not that big, collect all comments for each party into one large text
    conservative_comments_df_ = comments_df.filter(
        comments_df[PARTY_LABEL_FIELD]==functions.lit(CONSERVATIVE_LABEL)
    )
    conservative_comments_list = conservative_comments_df_.select(functions.explode(
        conservative_comments_df_['filtered_tokens']
    )).rdd.collect()
    conservative_comments = " ".join([" ".join(comment) for comment in conservative_comments_list])

    liberal_comments_df_ = comments_df.filter(
        comments_df[PARTY_LABEL_FIELD]==functions.lit(LIBERAL_LABEL)
    )
    liberal_comments_list = liberal_comments_df_.select(functions.explode(
        liberal_comments_df_['filtered_tokens']
    )).rdd.collect()
    liberal_comments = " ".join([" ".join(comment) for comment in liberal_comments_list])

    # Customize the word cloud
    wordcloud = WordCloud(width=800, height=400,
        background_color='white',
        max_words=50,
        max_font_size=60,
        colormap='magma',
        prefer_horizontal=0.5
    ).generate(conservative_comments)

    # Store the word cloud
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(output+'/conservative_wordcloud.png', format="png")
    # plt.show()

    # Customize the word cloud
    wordcloud = WordCloud(width=800, height=400,
        background_color='white',
        max_words=50,
        max_font_size=60,
        colormap='magma',
        prefer_horizontal=0.5
    ).generate(liberal_comments)

    # Store the word cloud
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(output+'/liberal_wordcloud.png', format="png")
    # plt.show()


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages 
    spark = SparkSession.Builder() \
        .appName('Word Cloud') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)