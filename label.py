#run this:
#spark-submit transformation.py reddit-2021 labeled-reddit-2021
import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import re


def main(input, output):
    comments = spark.read.parquet(f"{input}/comments")
    submissions = spark.read.parquet(f"{input}/submissions")
    
    
    submissions = submissions.withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                            .withColumn("datetime", from_utc_timestamp("datetime", "America/Los_Angeles"))
        
    comments = comments.withColumn("datetime", from_unixtime("created_utc").cast(TimestampType()))\
                    .withColumn("datetime", from_utc_timestamp("datetime", "America/Los_Angeles"))
  
    liberal_keywords = ["trudeau", "justin trudeau", "liberals", "liberal party", "libparty", "justintrudeau"]
    liberal_pattern = '|'.join(re.escape(word) for word in liberal_keywords)

    conservative_keywords = ["conservatives", "conservative party", "scheer", "andrew scheer", "o'toole", "erin o'toole"]
    conservative_pattern = '|'.join(re.escape(word) for word in conservative_keywords)

    comments = comments.withColumn("is_conservative", when(col("body").rlike(conservative_pattern), 1).otherwise(0))\
        .withColumn("is_liberal", when(col("body").rlike(liberal_pattern), 1).otherwise(0))
    submissions = submissions.withColumn("is_conservative", when(col("title").rlike(conservative_pattern), 1).otherwise(0))\
        .withColumn("is_liberal", when(col("title").rlike(liberal_pattern), 1).otherwise(0))
    

    comments = comments.withColumn(
    "label",
    when((col("is_liberal") == 1) & (col("is_conservative") == 0), "liberal")
    .when((col("is_liberal") == 0) & (col("is_conservative") == 1), "conservative")
    .when((col("is_liberal") == 1) & (col("is_conservative") == 1), "both")
    .otherwise("neither")
    )
    
    #seperate comments that contains keywords from both parties, comment out for now, might add it back
    """
    comments_segment = (
    comments.filter(col("label") == "both")
    .withColumn("body", explode(split(col("body"), "[.,;!?]")))
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
    
    comments = comments.filter(col("label")!="both").union(comments_segment)
    #comments.groupBy("label").agg(count("*")).show()
    #comments.filter(col("label") == "both").select(["label", "body"]).show(10, truncate = False)
    
    """
    comments.write.mode("overwrite").parquet(f"{output}/comments")
    submissions.write.mode("overwrite").parquet(f"{output}/submissions")
    
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('reddit transformation').getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", "1000")
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)