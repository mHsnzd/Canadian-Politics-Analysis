'''
This script is automatically triggered whenever daily transformed data is added to the S3 bucket to perform some basic aggregation.

The script isn't run locally, it is a lambda function ran using Amazon Lambda
'''

import boto3

def lambda_handler(event, context):

    # Connect to clients
    client = boto3.client('athena')
    s3_client = boto3.client('s3')

    # Extract the file path from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    if 'submission' in s3_key:
        return {
            'statusCode': 200,
            'body': "The input file does not contian new comments."
        }
        
    # Define the SQL query to be implemented on the new parquet
    query = f"""
        SELECT 
            year,
            month,
            day,
            label,
            sentiment,
            emotion,
            hate_speech,
            AVG(sentiment_score) AS average_sentiment_score,
            COUNT(sentiment) AS comment_count,
            AVG(score) AS average_upvotes
        FROM 
            reddit.nlp
        WHERE 
            "$path" = 's3://{s3_bucket}/{s3_key}'
        GROUP BY 
            year, 
            month,
            day, 
            label, 
            sentiment, 
            emotion, 
            hate_speech
        ORDER BY 
            year, 
            month,
            day, 
            label, 
            sentiment, 
            emotion, 
            hate_speech;
    """

    # Start query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'reddit'},
        ResultConfiguration={'OutputLocation': 's3://cmpt732-reddit-daily/output/temporary'}
    )

    # Wait for the query to complete
    query_execution_id = response['QueryExecutionId'] 
    
    status = client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
    while status in ['RUNNING', 'QUEUED']:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']

    # Define output paths
    s3_ouput_bucket = 'cmpt732-reddit-daily'
    s3_output_tmp_key = f'output/temporary/{query_execution_id}.csv'
    s3_output_final_key = 'output/reddit-final-nlp.csv'
    
    try:
        # Download the temp file from S3
        tmp_file_obj = s3_client.get_object(Bucket=s3_ouput_bucket, Key=s3_output_tmp_key)
        tmp_file_content = tmp_file_obj['Body'].read().decode('utf-8')

        # Skip the header line
        lines = tmp_file_content.splitlines()
        content_to_append = '\n'.join(lines[1:])

        # Download the final file form s3  
        final_file_obj = s3_client.get_object(Bucket=s3_ouput_bucket, Key=s3_output_final_key)
        final_file_content = final_file_obj['Body'].read().decode('utf-8')

        # Append the daily data to final data
        updated_final_content = final_file_content + '\n' + content_to_append

        # Upload the final file in S3
        s3_client.put_object(Bucket=s3_ouput_bucket, Key=s3_output_final_key, Body=updated_final_content.encode('utf-8'))

        return {
            'statusCode': 200,
            'body': "Successfully appended the comments data."
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"An error occurred: {str(e)}"
        }


