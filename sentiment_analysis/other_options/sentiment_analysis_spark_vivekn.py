'''
This script performs sentiment analysis on the labeled comments using Spark NLP. A pretrained model based on the approach by Vivek Narayanan is used: https://sparknlp.org/2021/11/22/sentiment_vivekn_en.html

This model can only give positive or negative labels, no scores are given.

To locally run the script use the command:
spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1 sentiment_analysis_spark.py input-parquet-path output-parquet-path

'''


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row 
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


# Constants:
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal'] 
SENTIMENT_LABEL_FIELD = 'sentiment'


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)
    # Ignore neither or both labels
    comments_df = comments_df.filter(comments_df[PARTY_LABEL_FIELD].isin(PARTY_LABELS))

    # Convert the text column of the dataframe into the acceptable input for the model 
    document = DocumentAssembler() \
        .setInputCol(COMMENT_TXT_FIELD) \
        .setOutputCol('document')
    
    token = Tokenizer() \
        .setInputCols(['document']) \
        .setOutputCol('token')

    normalizer = Normalizer() \
        .setInputCols(['token']) \
        .setOutputCol('normal')

    # Use the pre-trained sentiment-analysis-vivekn model
    vivekn =  ViveknSentimentModel.pretrained() \
        .setInputCols(['document', 'normal']) \
        .setOutputCol('result_sentiment')

    finisher = Finisher() \
        .setInputCols(['result_sentiment']) \
        .setOutputCols(SENTIMENT_LABEL_FIELD)

    # Build the pipeline from all the stages
    pipeline = Pipeline().setStages([document, token, normalizer, vivekn, finisher])

    # Fit and transform the data
    model = pipeline.fit(comments_df)
    result = model.transform(comments_df)

    # Write the result to a Parquet file
    # result.show(truncate=False)
    result.write.parquet(output, mode='overwrite')


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.builder \
        .appName('Sentiment Analysis Spark NLP') \
        .config('spark.jars.packages', 'com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)