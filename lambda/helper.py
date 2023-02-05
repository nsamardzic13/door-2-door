import json
import pandas as pd
from connections import LandingBucket, DataLakeBucket, Postgres


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

class Helper():

    def __init__(self, pg: Postgres, s3_landing: LandingBucket, s3_datalake: DataLakeBucket) -> None:
        self.pg = pg
        self.s3_landing = s3_landing
        self.s3_datalake = s3_datalake
        self.successful_files = []
        self.failed_files = {}

    def update_df(self, df: pd.DataFrame, event: str) -> pd.DataFrame:
        df = df[config[event]['toKeep']].copy()
        df.rename(
            config[event]['rename'], 
            axis='columns',
            inplace=True
        )
        return df
    
    def insert_data(self, df: pd.DataFrame) -> None:
        df['at'] = pd.to_datetime(df['at'], format='%Y-%m-%dT%H:%M:%S.%f%z')

        df_update = df.loc[(df['event'] == 'update') & (df['on'] == 'vehicle')]
        if not df_update.empty:
            df_update = self.update_df(df_update.copy(), 'vehicleLocation')
            self.pg.exec_insert(
                df=df_update, 
                schema=config['vehicleLocation']['targetSchema'],
                table=config['vehicleLocation']['targetTable']
            )

        df_register = df.loc[(df['event'] != 'update') & (df['on'] == 'vehicle')]
        if not df_register.empty:
            df_register = self.update_df(df_register.copy(), 'vehicleRegister')
            self.pg.exec_insert(
                df=df_register, 
                schema=config['vehicleRegister']['targetSchema'],
                table=config['vehicleRegister']['targetTable']
            )

        df_operating_period = df.loc[df['on'] == 'operating_period']
        if not df_operating_period.empty:
            df_operating_period = self.update_df(df_operating_period.copy(), 'operatingPeriod')
            self.pg.exec_insert(
                df=df_operating_period, 
                schema=config['operatingPeriod']['targetSchema'],
                table=config['operatingPeriod']['targetTable']
            )

    def process_s3_files(self, data_list: list) -> None:
        for i, file in enumerate(data_list, 1):
            print(f'Starting {i}/{len(data_list)}: {file}')
            try:
                obj = self.s3_landing.client.get_object(
                    Bucket=self.s3_landing.bucket_name,
                    Key=file
                )
                json_content = obj['Body'].read().decode('utf-8').splitlines()
                json_content = list(map(json.loads, json_content))
                df = pd.json_normalize(json_content)
                self.insert_data(df)
                self.successful_files.append(file)
            except Exception as e:
                self.failed_files[file] = e

    def move_to_datalake(self) -> None:
        try:
            for file in self.successful_files:
                self.s3_landing.client.copy_object(
                    Bucket=self.s3_datalake.bucket_name, 
                    CopySource={
                        "Bucket": self.s3_landing.bucket_name, 
                        "Key": file
                    }, 
                    Key=file
                )

                # self.s3_landing.client.delete_object(
                #     Bucket=self.s3_landing.bucket_name, 
                #     Key=file
                # )
        except Exception as e:
            print(f'Moving file to DL failed: {e}')
            raise e
