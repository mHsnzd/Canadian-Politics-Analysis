import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import re
#The reddit-2021-subs-sample contains the comments and submissions files from year 2021 and the predifined subreddits.


def main(input, output):
    comments = spark.read.parquet(f"{input}/comments")
    submissions = spark.read.parquet(f"{input}/submissions")
    
    submissions = submissions.select("id", "title", "subreddit")\
        .withColumn("id", concat(lit("t3_"), col("id")))\
        .withColumnRenamed("id", "submission_id")\
            .filter((col("subreddit") != "Liberal") & (col("subreddit") != "Conservative"))
        
    comments = comments.select("parent_id", "id", "link_id", "created_utc", "body", "subreddit")\
        .withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
        .withColumn("datetime", from_utc_timestamp("datetime", "America/Los_Angeles"))\
                        .filter((col("subreddit") != "Liberal") | (col("subreddit") != "Conservative"))
    
    joined_df = comments.join(submissions, comments.link_id == submissions.submission_id)
    joined_df = joined_df.withColumn("title_body", concat(joined_df["title"], lit(" "), joined_df["body"]))
    joined_df = joined_df.withColumn("title_body", lower(joined_df["title_body"]))
    
    liberal_keywords = ["trudeau", "justin trudeau", "liberals", "liberal party", "libparty", "justintrudeau"]
    liberal_pattern = '|'.join(re.escape(word) for word in liberal_keywords)

    conservative_keywords = ["conservatives", "conservative party", "scheer", "andrew scheer", "o'toole", "erin o'toole"]
    conservative_pattern = '|'.join(re.escape(word) for word in conservative_keywords)

    joined_df = joined_df.withColumn("is_conservative", when(col("title_body").rlike(conservative_pattern), 1).otherwise(0))\
        .withColumn("is_liberal", when(col("title_body").rlike(liberal_pattern), 1).otherwise(0))
    
    joined_df = joined_df.withColumn(
    "label",
    when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberals")
    .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservatives")
    .when((col("is_liberal") == 1) & (col("is_conservative") == 1), "both")
    .otherwise("neither")
    )
    
    joined_df = joined_df.filter((col("label") == "conservatives") | (col("label") == "liberals"))\
        .select("id", "datetime", "title_body", "label")
    joined_df.write.mode("overwrite").parquet(f"{output}")
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('reddit transformation').getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)