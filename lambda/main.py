import json 
from connections import Postgres, LandingBucket, DataLakeBucket
from helper import Helper


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

def lambda_handler(event, context):
    lnd_bucket = LandingBucket()
    data_lake_bucket = DataLakeBucket()
    pg = Postgres()
    helper = Helper(
        pg=pg,
        s3_landing=lnd_bucket,
        s3_datalake=data_lake_bucket
    )

    source_file_list = lnd_bucket.get_file_list()

    helper.process_s3_files(
        data_list=source_file_list
    )
    
    # execute stored procedures
    for sp in config['storedProcedures']:
        pg.execute_stored_procedure(sp)
    print('Procedures executed successfully')

    # move successful files to DL
    helper.move_to_datalake()
    print('Files moved to DataLake')

    # check if any files failed
    if helper.failed_files:
        print(f'Following files failed: {helper.failed_files}')
        raise helper.failed_files
    else:
        # truncate stagining tables
        for table in config['tablesToTruncate']:
            pg.truncate_stg_table(table)
        print('Truncate tables successfully')