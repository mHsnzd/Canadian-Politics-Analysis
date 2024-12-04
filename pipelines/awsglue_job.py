import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import lower, when, col
from pyspark.sql.types import StringType

from awsglue import DynamicFrame  # Import DynamicFrame


from transformers import pipeline

# Constants
COMMENT_TXT_FIELD = 'body'
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal']
MAX_SEQ_LENGTH = 512

# AWS Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext()
spark = glueContext.spark_session

# Set Spark configuration to handle datetime format
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input parameters
input_date = '20241114'
data_dir = 's3://cmpt732'

# Paths to read data
comments_path = f"{data_dir}/test_input/reddit_comments_{input_date}.parquet"
submissions_path = f"{data_dir}/test_input/reddit_submissions_{input_date}.parquet"

# Read both files into separate DataFrames
comments_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [comments_path], "recurse": False},
    transformation_ctx="comments_df"
).toDF()

submissions_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [submissions_path], "recurse": False},
    transformation_ctx="submissions_df"
).toDF()

# Label data based on keywords
def label_data(comments, submissions):
    liberal_keywords = ["trudeau", "justin trudeau", "liberals", "liberal party", "libparty", "justintrudeau"]
    conservative_keywords = ["conservatives", "conservative party", "scheer", "andrew scheer", "o'toole", "erin o'toole"]

    liberal_pattern = '|'.join(re.escape(word) for word in liberal_keywords)
    conservative_pattern = '|'.join(re.escape(word) for word in conservative_keywords)

    comments = comments.withColumn("body", lower(col("body"))) \
        .withColumn("is_conservative", when(col("body").rlike(conservative_pattern), 1).otherwise(0)) \
        .withColumn("is_liberal", when(col("body").rlike(liberal_pattern), 1).otherwise(0))

    submissions = submissions.withColumn("title", lower(col("title"))) \
        .withColumn("is_conservative", when(col("title").rlike(conservative_pattern), 1).otherwise(0)) \
        .withColumn("is_liberal", when(col("title").rlike(liberal_pattern), 1).otherwise(0))

    comments = comments.withColumn(
        "label",
        when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
        .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
        .otherwise("neither")
    )

    submissions = submissions.withColumn(
        "label",
        when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
        .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
        .otherwise("neither")
    )
    return comments, submissions

# Sentiment Analysis using Hugging Face Transformers
def sentiment_analysis(comments_df):
    model = pipeline(
        'sentiment-analysis',
        model='distilbert-base-uncased-finetuned-sst-2-english'
    )

    def analyze_sentiment(comment):
        truncated_comment = comment[:MAX_SEQ_LENGTH]
        result = model(truncated_comment)[0]
        return result['label']

    # Register UDF
    sentiment_udf = F.udf(analyze_sentiment, StringType())

    # Apply UDF
    comments_df = comments_df.withColumn('sentiment', sentiment_udf(F.col(COMMENT_TXT_FIELD)))
    return comments_df

# Label and analyze
labeled_comments, labeled_submissions = label_data(comments_df, submissions_df)
labeled_comments = sentiment_analysis(labeled_comments)

# Convert DataFrames back to DynamicFrames for writing
labeled_comments_dynamic = DynamicFrame.fromDF(labeled_comments, glueContext, "labeled_comments_dynamic")
labeled_submissions_dynamic = DynamicFrame.fromDF(labeled_submissions, glueContext, "labeled_submissions_dynamic")



# Write output back to S3
glueContext.write_dynamic_frame.from_options(
    frame=labeled_comments_dynamic,
    connection_type="s3",
    format="parquet",
    connection_options={"path": f"{data_dir}/test/reddit_comments_{input_date}", "partitionKeys": []},
    transformation_ctx="labeled_comments"
)

glueContext.write_dynamic_frame.from_options(
    frame=labeled_submissions_dynamic,
    connection_type="s3",
    format="parquet",
    connection_options={"path": f"{data_dir}/test/reddit_submissions_{input_date}", "partitionKeys": []},
    transformation_ctx="labeled_submissions"
)

# Commit job
job.commit()
