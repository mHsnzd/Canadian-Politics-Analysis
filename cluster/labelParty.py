#run this:
#spark-submit labelParty.py reddit-2019/submissions /user/tga63/clean_comments_2019 labele-reddit-2019-new
import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import re


def main(submissions_path, comments_path, output):
    submissions = spark.read.parquet(submissions_path)
    comments = spark.read.parquet(comments_path)
    
    #check if datetime aligns with year and month
    submissions = submissions.drop("year", "month")\
                    .withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                    .withColumn("datetime", from_utc_timestamp("datetime", "America/Los_Angeles"))\
                    .withColumn("year", year("datetime")) \
                    .withColumn("month", month("datetime")) \
                    .withColumn("day", dayofmonth("datetime"))
        
    comments = comments.drop("year", "month")\
                    .withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                    .withColumn("datetime", from_utc_timestamp("datetime", "America/Los_Angeles"))\
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
    
    comments.drop("is_liberal", "is_conservative").write.mode("overwrite").partitionBy("month", "day").parquet(f"{output}/comments")
    submissions.drop("is_liberal", "is_conservative").write.mode("overwrite").partitionBy("month", "day").parquet(f"{output}/submissions")
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('reddit transformation').getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    submissions_path = sys.argv[1]
    comments_path  = sys.argv[2]
    output = sys.argv[3]
    main(submissions_path, comments_path, output)