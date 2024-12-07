"""
This script outputs the number of rows or comments present in different input datasets. 

To locally run the script use the command:
spark-submit row_count.py input-parquet-path1 input-parquet-path2 ...
"""


import sys
assert sys.version_info >= (3, 5) # Make sure we have Python 3.5+
from pyspark.sql import SparkSession


def main(inputs):

    for input in inputs:

        # Read number of rows from files in this path
        rows = spark.read.parquet(input).count()

        # Print number of rows for this path
        print(f'Number of rows in {input}: {rows}')
        print('-'*50)


if __name__ == '__main__':
    # Reading the  command line arguments
    # output = sys.argv[-1] if sys.argv[1]=='--store' else None
    # inputs = sys.argv[1:-1] if output else sys.argv[1:]
    inputs = sys.argv[1:]

    # Initializing sparksession with necessary packages 
    spark = SparkSession.Builder() \
        .appName('Historical Data Metadata') \
        .getOrCreate()
    assert spark.version >= '3.0' # Make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    main(inputs)