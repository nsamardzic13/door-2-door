import json
import os

import boto3


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)


# setup boto3
session = boto3.Session(profile_name=config['profileName'])
client = session.client('s3')

# set relative path to the data folder
DATA_FOLDER_PATH = '../data/'
S3_FOLDER = 'VEHICLE'

files_to_upload = os.listdir(DATA_FOLDER_PATH)
number_of_files = len(files_to_upload)

# create lists and add potential failed uploads
failed_files = []
for i, file in enumerate(files_to_upload, start=1):
    print(f'{i}/{number_of_files}...Uploading {file}')
    try:
        client.upload_file(
            os.path.join(DATA_FOLDER_PATH, file),
            config['targetBucketName'],
            f'{S3_FOLDER}/{file}'
        )
    except Exception as e:
        failed_files.append({
            file: e
        })

# print upload result
if not failed_files:
    print('Files uploaded successfuly')
else:
    print(f'{len(failed_files)} errors occured')
    print(failed_files)
