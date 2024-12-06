"""
This script performs sentiment analysis on the labeled comments using Spark NLP. A pretrained model based on tweets: https://sparknlp.org/2021/01/18/sentimentdl_use_twitter_en.html

To locally run the script use the command:
spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1 --master local[4] --executor-memory 4g sentiment_analysis_spark.py input-parquet-path output-parquet-path




"""

import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row 
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)

    # Convert the title_body column of the dataframe into the acceptable input for the model 
    document = DocumentAssembler() \
        .setInputCol("title_body") \
        .setOutputCol("document")
    
    use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
        .setInputCols(["document"])\
        .setOutputCol("sentence_embeddings")

    classifier = SentimentDLModel().pretrained('sentimentdl_use_twitter')\
        .setInputCols(["sentence_embeddings"])\
        .setOutputCol("sentiment")

    pipeline = Pipeline(stages=[document,
        use,
        classifier
        ])

    # Fit and transform the data
    model = pipeline.fit(comments_df)
    result = model.transform(comments_df)

    # Write the result to a Parquet file
    result.show(truncate=False)
    # result.repartition(20).write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.builder \
        .appName('Sentiment Analysis Spark NLP') \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)