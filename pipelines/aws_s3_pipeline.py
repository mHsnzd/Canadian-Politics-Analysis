from datetime import datetime

from etls.aws_etl import create_dataset, connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import *


def upload_s3_pipeline(dt: datetime):
    file_path = create_dataset(dt)
    s3 = connect_to_s3()
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])