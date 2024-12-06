import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import re

COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal'] 
COMMENT_ID_FIELD = 'id'

def read_data(path, type = ""):
    df = spark.read.parquet(path)

    df = df.withColumn('created_utc', (functions.col('created_utc').cast('long') / functions.lit(10**9)))
    
    if type == "comments":
        return clean_data(df)
    return df

def clean_data(df):   
    df_cleaned = df.withColumn('body', functions.regexp_replace(functions.col('body'), r'[\n\r\t]', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'http[s]?://\S+', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'www\.\S+', ''))

    df_cleaned = df_cleaned.withColumn('body', functions.regexp_replace(functions.col('body'), r'[\\/]', ''))

    df_cleaned = df_cleaned.filter(functions.col('body').isNotNull() & (functions.length(functions.col('body')) > 0) & \
    (~functions.lower(functions.col('body')).isin('[deleted]', '[removed]')))

    return df_cleaned 

def label_data(comments, submissions):   
    #check if datetime aligns with year and month
    submissions = submissions.drop("year", "month")\
                    .withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                    .withColumn("datetime", from_utc_timestamp("datetime", "UTC"))\
                    .withColumn("year", year("datetime")) \
                    .withColumn("month", month("datetime")) \
                    .withColumn("day", dayofmonth("datetime"))
        
    comments = comments.drop("year", "month")\
                    .withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                    .withColumn("datetime", from_utc_timestamp("datetime", "UTC"))\
                    .withColumn("year", year("datetime")) \
                    .withColumn("month", month("datetime")) \
                    .withColumn("day", dayofmonth("datetime"))
  
    liberal_keywords = ["trudeau", "justin trudeau", "liberals", "liberal party", "libparty", "justintrudeau"]
    liberal_pattern = '|'.join(re.escape(word) for word in liberal_keywords)

    conservative_keywords = ["conservatives", "conservative party", "scheer", "andrew scheer", "o'toole", "erin o'toole"]
    conservative_pattern = '|'.join(re.escape(word) for word in conservative_keywords)
    
    comments = comments.withColumn("body", lower(comments["body"]))\
        .withColumn("is_conservative", when(col("body").rlike(conservative_pattern), 1).otherwise(0))\
        .withColumn("is_liberal", when(col("body").rlike(liberal_pattern), 1).otherwise(0))
        
    submissions = submissions.withColumn("title", lower(submissions["title"]))\
        .withColumn("is_conservative", when(col("title").rlike(conservative_pattern), 1).otherwise(0))\
        .withColumn("is_liberal", when(col("title").rlike(liberal_pattern), 1).otherwise(0))

    comments = comments.withColumn(
    "label",
    when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
    .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
    .when((col("is_liberal") == 1) & (col("is_conservative") == 1), "both")
    .otherwise("neither")
    )
    
    submissions = submissions.withColumn(
    "label",
    when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
    .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
    .when((col("is_liberal") == 1) & (col("is_conservative") == 1), "both")
    .otherwise("neither")
    )
    
    comments_segment = (
    comments.filter(col("label") == "both")
    .withColumn("body", explode(split(col("body"), "[.]")))
    .filter(col("body").rlike("\\S"))
    )
    
    comments_segment = comments_segment.withColumn("is_conservative", when(col("body").rlike(conservative_pattern), 1).otherwise(0))\
    .withColumn("is_liberal", when(col("body").rlike(liberal_pattern), 1).otherwise(0))
    
    comments_segment = comments_segment.withColumn(
    "label",
    when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
    .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
    .when((col("is_liberal") == 1) & (col("is_conservative") == 1), "both")
    .otherwise("neither")
    )

    comments = comments.filter(col("label")!="both").union(comments_segment).drop("is_liberal", "is_conservative")

    comments = comments.drop("is_liberal", "is_conservative")
    submissions = submissions.drop("is_liberal", "is_conservative")

    return comments, submissions



def transform_data(comments_path, submissions_path):
    comments = read_data(comments_path, "comments")
    submissions = read_data(submissions_path, "submissions") 
    labeled_comments, labeled_submissions = label_data(comments, submissions) 

    return labeled_comments, labeled_submissions

def main(input_date, dir):
    comments_path = f"{dir}/reddit_comments_{input_date}.parquet"
    submissions_path = f"{dir}/reddit_submissions_{input_date}.parquet"
    
    # Create the transform directory if it doesn't exist
    os.makedirs(f"{dir}/cleaned", exist_ok=True)
    
    # Perform data transformation
    comments, submissions = transform_data(comments_path, submissions_path)
    
    # Save labeled_comments and labeled_submissions
    comments.coalesce(1).write.option("header", "true").mode("overwrite").parquet(f"{dir}/cleaned/reddit_comments_{input_date}")
    submissions.coalesce(1).write.option("header", "true").mode("overwrite").parquet(f"{dir}/transformed/reddit_submissions_{input_date}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName('clean reddit data').getOrCreate()
    assert spark.version >= '3.0'  # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    input_date = sys.argv[1]  # Date passed as command-line argument
    data_dir = sys.argv[2]
    main(input_date,data_dir)
