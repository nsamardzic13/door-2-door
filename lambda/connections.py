import json
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

# load SM
session = boto3.session.Session()
client = session.client('secretsmanager')

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=config["secretName"]
    )
except Exception as e:
    raise e

secret = json.loads(get_secret_value_response['SecretString'])


# create postgres class
# on init, create connection and cursor
class Postgres():
    
    def __init__(self) -> None:
        self.engine = create_engine(
            f"postgresql://{secret['username']}:{secret['password']}@{config['rds']['host']}:{config['rds']['port']}/{config['rds']['database']}",
            connect_args={"connect_timeout": 15}
        )

    def exec_insert(self, df: pd.DataFrame, schema: str, table: str) -> None:
        df.to_sql(
            con=self.engine,
            schema=schema,
            name=table,
            if_exists='append',
            index=False
        )

    def execute_stored_procedure(self, call_txt: str) -> None:
        try:
            with Session(self.engine) as session, session.begin():
                session.execute(
                    text(
                        call_txt
                    ).execution_options(autocommit=True)
                )
        except Exception as e:
            print(f'Execution of sp failed {e}')
            raise e

    def truncate_stg_table(self, table: str) -> None:
        try:
            with Session(self.engine) as session, session.begin():
                session.execute(
                    text(
                        f'truncate table {table}'
                    ).execution_options(autocommit=True)
                )
        except Exception as e:
            print(f'Execution of truncate failed {e}')
            raise e


# create bucket class
class LandingBucket():
    
    def __init__(self) -> None:
        self.session = boto3.session.Session()
        self.client = self.session.client('s3')
        self.bucket_name = config['landingBucketName']
        self.prefix = config['bucketPrefix']

    def get_file_list(self) -> list:
        all_objects = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=self.prefix,
        )
        
        files = [obj['Key'] for obj in all_objects['Contents']]
        return files


class DataLakeBucket():
    
    def __init__(self) -> None:
        self.session = boto3.session.Session()
        self.client = self.session.client('s3')
        self.bucket_name = config['dataLakeBucketName']
     