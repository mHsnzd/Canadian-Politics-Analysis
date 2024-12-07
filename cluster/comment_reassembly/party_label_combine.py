"""
This script recombines comments segments for the comments that were broken down into several parts in the labelling process.

From this point forward comments are not uniquely identified by id, but by the combination of (id, label).

To locally run the script use the command:
spark-submit party_label_combine.py input-parquet-path output-parquet-path
"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from transformers import pipeline
from pyspark.sql import SparkSession, functions, types, Row


# Constants
COMMENT_TXT_FIELD = 'body' 
PARTY_LABEL_FIELD = 'label'
PARTY_LABELS = ['conservative', 'liberal'] 
COMMENT_ID_FIELD = 'id'
PARTITION_BY_FIELDS = ['subreddit','year','month','day']
FINAL_UNIQUE_ID = 'unique_id'


def main(input, output):
    # Read data from files
    comments_df = spark.read.parquet(input)
    # Ignore neither or both labels
    comments_df = comments_df.filter(comments_df[PARTY_LABEL_FIELD].isin(PARTY_LABELS)).cache()
    
    # Re-combine the comments that were broken down into several parts in the labelling stage
    combined_comments_df = comments_df\
        .groupBy(COMMENT_ID_FIELD, PARTY_LABEL_FIELD)\
        .agg(functions.concat_ws(
            '. ',
            functions.collect_list(COMMENT_TXT_FIELD)
        ).alias(COMMENT_TXT_FIELD))
    
    # Join the combined comments back to the dataframe
    comments_df = comments_df.drop(COMMENT_TXT_FIELD).dropDuplicates()
    result_df = comments_df.join(
        combined_comments_df,
        on=[COMMENT_ID_FIELD, PARTY_LABEL_FIELD] 
    )
    
    # result_df.explain()

    # Write the result to a Parquet file
    # result.show(truncate=False)
    result_df.write\
        .partitionBy(PARTITION_BY_FIELDS)\
        .parquet(output, mode='overwrite')  


if __name__ == '__main__':
    # Reading the  command line arguments
    input = sys.argv[1] 
    output = sys.argv[2]        

    # Initializing sparksession with necessary packages (Alternatively use spark = sparknlp.start())
    spark = SparkSession.Builder() \
        .appName('Comment Reassembly') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(input, output)