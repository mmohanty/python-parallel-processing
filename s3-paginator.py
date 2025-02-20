import boto3

# List All Objects in an S3 Bucket
s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'

paginator = s3_client.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket=bucket_name):
    if 'Contents' in page:
        for obj in page['Contents']:
            print(obj['Key'])

# List All Objects in an S3 Bucket

paginator = s3_client.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket=bucket_name, Prefix='your-prefix/'):
    if 'Contents' in page:
        for obj in page['Contents']:
            print(obj['Key'])


# 3. Delete All Objects in a Bucket (Batch Processing)
import boto3

s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'

paginator = s3_client.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket=bucket_name):
    if 'Contents' in page:
        delete_objects = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_objects)

#4. Download All Files in an S3 Bucket
import boto3
import os

s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'
local_directory = 'downloaded_files/'

os.makedirs(local_directory, exist_ok=True)

paginator = s3_client.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket=bucket_name):
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            local_file_path = os.path.join(local_directory, os.path.basename(key))
            s3_client.download_file(bucket_name, key, local_file_path)
            print(f"Downloaded {key} to {local_file_path}")

# 5. Upload Multiple Files Using Paginator

import boto3
import os

s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'
local_directory = 'upload_directory/'

for root, dirs, files in os.walk(local_directory):
    for file in files:
        file_path = os.path.join(root, file)
        s3_client.upload_file(file_path, bucket_name, file)
        print(f"Uploaded {file_path} to S3 bucket {bucket_name}")

#. Generate Pre-Signed URLs for All Files
import boto3

s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'

paginator = s3_client.get_paginator('list_objects_v2')

for page in paginator.paginate(Bucket=bucket_name):
    if 'Contents' in page:
        for obj in page['Contents']:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': obj['Key']},
                ExpiresIn=3600  # URL expires in 1 hour
            )
            print(f"{obj['Key']}: {presigned_url}")


