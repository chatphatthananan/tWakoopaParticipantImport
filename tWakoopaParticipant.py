import pandas as pd
import requests
import datetime
import hmac
import hashlib
import string
import random
import logging
from SGTAMProdTask import SGTAMProd
from datetime import datetime, date, timedelta, timezone
import config
from sqlalchemy import create_engine
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import lit, current_date, col, unix_timestamp, from_unixtime, date_format, current_timestamp

try:
    # Set up logging
    log_filename = f"D:/SGTAM_DP/Working Project/Wakoopa/tWakoopaParticipantImport/log/tWakoopaParticipant_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
    logging.basicConfig(filename=log_filename, level=logging.INFO)
    s = SGTAMProd()

    config.SGTAM_log_config['statusFlag'], config.SGTAM_log_config['logID']  = s.insert_tlog(**config.SGTAM_log_config)

    #------------------------------------------------------------------------------------------------------#
    # Define functions                                                                                     #
    #------------------------------------------------------------------------------------------------------#

    def random_string(length):
        return ''.join(random.choice(string.ascii_letters) for m in range(length))
    #------------------------------------------------------------------------------------------------------#

    #------------------------------------------------------------------------------------------------------#
    # This part is to get data from API and store them in Pandas df first                                  #
    #------------------------------------------------------------------------------------------------------#
    current_time = datetime.now(timezone.utc)
    utc_time = current_time.replace(tzinfo=timezone.utc)
    utc_timestamp = int(utc_time.timestamp())
    nonce = random_string(15)
    message = str(utc_timestamp) + nonce
    secret = 'xxx' # Wen Xuan's secret key 
    signature = hmac.new(bytes(secret, 'latin-1'), msg=bytes(message, 'latin-1'), digestmod=hashlib.sha256).hexdigest()
    client = 'xxx'
    api_key = f"{client}&nonce={nonce}&timestamp={utc_timestamp}=&signature={signature}"

    # Get details of all panelist
    get_all_participants = ( f"https://wakoopa.wkp.io/api/v1/participants/?"
                    f"&date_from='2023-03-31'"
                    f"&per_page=999999999999"
                    f"&include=devices"
                    f"&api_key={client}&nonce={nonce}&timestamp={utc_timestamp}&signature={signature}" )

    print(get_all_participants)

    try:
        print('Retrieving participants informations from the API')
        logging.info('Retrieving participants informations from the API')

        response = requests.get(get_all_participants)
    
        # Status code 200 means successful
        if response.status_code == 200: 
            # Parse the JSON response into a Python dictionar
            print('Response status code is 200, connection to API is successful.')
            logging.info('Response status code is 200, connection to API is successful.')

            data = response.json()

            df = pd.DataFrame(data['participants'])
            

        else:
            raise Exception(f'Error: {response.status_code}, exiting')

    except requests.exceptions.RequestException as e:
        raise e

    # Assuming df is your DataFrame containing the column "links" with dictionaries
    # Replace 'your_column_name' with the actual name of the column containing the dictionaries
    # Replace 'your_desired_url_key' with the actual key for the desired URL within the dictionaries
    def extract_url(row):
        try:
            for param in row['links']['configuration_parameters']:
                #print(param)
                if param['id'] == 'configurator_login_url':
                    return param['contents']
        except (KeyError, IndexError):
            return None

    # Apply the function to extract the URL
    print('Extracting the profile links information from the nested dictionary.')
    logging.info('Extracting the profile links information from the nested dictionary.')
    df['links'] = df.apply(extract_url, axis=1)
    df.rename(columns={'links': 'profile_url'}, inplace=True)
    df['import_date'] = datetime.today().date()

    df = df[['import_date','id','tags','time_zone','created_at','profile_url']]

    # Define connection parameters
    server_name = 'xxx'
    database_name = 'xxx'
    username = 'xxx'
    password = 'xxx'

    # Construct the connection string
    connection_string = f'mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=SQL+Server'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Define the table name in your SQL Server database
    table_name = 'tWakoopaParticipants'

    # Define chunksize for processing data in chunks
    chunksize = 50  # Adjust as needed based on your system's memory constraints

    # Clear the table before importing data
    print('Clearing tWakoopaParticipants table before data import.')
    logging.info('Clearing tWakoopaParticipants table before data import.')
    with engine.connect() as conn:
        conn.execute(f'DELETE FROM {table_name}')

    # Iterate over the DataFrame in chunks and perform batch insertion
    print('Data import starting.')
    logging.info('Data import starting.')
    total_rows_inserted = 0
    counter = 1
    for chunk_start in range(0, len(df), chunksize):
        chunk_end = min(chunk_start + chunksize, len(df))
        chunk = df.iloc[chunk_start:chunk_end]
        total_rows_inserted += len(chunk)
        print(f"Insertion {counter} with chunk size of {len(chunk)} rows.")
        logging.info(f"Insertion {counter} with chunk size of {len(chunk)} rows.")
        # Insert chunk into the SQL table
        chunk.to_sql(table_name, engine, if_exists='append', index=False)
        counter += 1

    print(f"Total rows inserted: {total_rows_inserted}")
    logging.info(f"Total rows inserted: {total_rows_inserted}")
    logging.info(f"Log file attached in the email will not be completed as the file is attached and send before the scripts completed.")

    # Get today's date
    # Format the date as such example "21 March 2024" 
    today_date = datetime.today()
    formatted_date = today_date.strftime("%d %B %Y")

    config.email['to'] = 'xxx'
    config.email['subject'] = f"[OK] tWakoopaParticipants Import - {formatted_date}"
    config.email['body'] = f"The tWakoopaParticipants table was truncated and imported successfully for today.\n*This is an auto generated email, do not reply to this email."
    config.email['filename'] = f"{log_filename}"
    
    s.send_email(**config.email)
    logging.info('Email sent.')
    print('Email sent.')
    
    s.update_tlog(**config.SGTAM_log_config)
    logging.info('SGTAM log updated.')
    print('SGTAM log updated.')  

except Exception as e:
    print(f"An error occurred: {e}")
    logging.info(f"An error occurred: {e}")
    config.SGTAM_log_config['logMsg'] = f"An error occurred:\n{e}"
    config.SGTAM_log_config['statusFlag'] = 2
    config.email['to'] = 'xxx'
    config.email['subject'] = f"[ERROR] tWakoopaParticipants Import - {formatted_date}"
    config.email['body'] = f"An error occurred: \n{e} \n*This is an auto generated email, do not reply to this email."
    config.email['filename'] = f"{log_filename}"

    s.send_email(**config.email)
    logging.info('Email sent.')
    s.update_tlog(**config.SGTAM_log_config)
    logging.info('SGTAM log updated.')


finally:
    print('Enter finally clause.')
    logging.info('Enter finally clause.')
    # Dispose the engine
    engine.dispose()
    print('Ensure SQL Alchemy engine is stopped and disposed.')
    logging.info('Ensure SQL Alchemy engine is stopped and disposed.')