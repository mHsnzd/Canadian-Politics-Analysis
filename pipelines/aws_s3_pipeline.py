from datetime import datetime

from etls.aws_etl import create_dataset, connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import *


def upload_s3_pipeline(dt: datetime):
    comments_output_file, submissions_output_file = create_dataset(dt)
    s3 = connect_to_s3()
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, comments_output_file, AWS_BUCKET_NAME, comments_output_file.split('/')[-1])
    upload_to_s3(s3, submissions_output_file, AWS_BUCKET_NAME, submissions_output_file.split('/')[-1])