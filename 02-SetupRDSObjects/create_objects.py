import json
import os 

import boto3
import psycopg2


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

# load SM
session = boto3.session.Session(profile_name=config['profileName'])
client = session.client('secretsmanager')

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=config["secretName"]
    )
except Exception as e:
    raise e

secret = json.loads(get_secret_value_response['SecretString'])

# create pg connection
connection = psycopg2.connect(
        host=config['rds']['host'],
        database=config['rds']['database'],
        port=config['rds']['port'],
        user=secret['username'],
        password=secret['password'],
    )
# set autocommit
connection.autocommit = True

# create cursor
cursor = connection.cursor()

# execute queries
# create schemas
CREATE_SCHEMAS_FILE = 'queries/createSchemas.sql'
with open(CREATE_SCHEMAS_FILE, 'r') as f:
    full_file = f.read().split('\n\n')
    for query in full_file:
        try:
            cursor.execute(query)
            print(f'Query executed successfully: {query}')
        except Exception as e:
            print(f'{query} failed: {e}')

# create tables
CREATE_TABLES_FILE = 'queries/createTables.sql'
with open(CREATE_TABLES_FILE, 'r') as f:
    full_file = f.read().split('\n\n')
    for query in full_file:
        try:
            cursor.execute(query)
            print(f'Query executed successfully: {query}')
        except Exception as e:
            print(f'{query} failed: {e}')

# create procedures
create_procedure_files = [file for file in os.listdir('queries/') if file.startswith('createProcedure')]
for file in create_procedure_files:
    with open(os.path.join('queries', file), 'r') as f:
        full_file = f.read().split('\n\n\n')
        for query in full_file:
            try:
                cursor.execute(query)
                print(f'Query executed successfully: {query}')
            except Exception as e:
                print(f'{query} failed: {e}')

cursor.close()
connection.close()
