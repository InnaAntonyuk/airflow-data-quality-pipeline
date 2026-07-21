import logging
import pandas as pd
import numpy as np
import os
import requests
import math
import json
from datetime import datetime, timedelta
from airflow import DAG
from tempfile import NamedTemporaryFile
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from dotenv import load_dotenv

load_dotenv()
user_token= os.getenv('SLACK_USER_TOKEN')
channel_id= os.getenv('SLACK_CHANNEL_ID')


default_args = {
    'owner': 'Inna_Ant',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
    
def download_file(ds_nodash, ti):
    schedule_checking = {
        #---1-t week ---
        0:  ['FV_30', 'FV_11', 'FV_15'], # Sunday 21.06
        1:  ['FV_32', 'FV_33', 'FV_17', 'FV_29'], # Monday 22.06 ,
        2:  ['FV_30', 'FV_26', 'FV_6', 'FV_4'], # Tuesday 23.06
        3:  ['FV_28', 'FV_18', 'FV_3', 'FV_23'], # Wednesday 24.06
        4:  ['FV_11', 'FV_7', 'FV_22'], # Thursday 25.06
        5:  [], # Friday(day-off)
        6:  [], # Saturday(day-off)

        # --- 2-d week  ---
        7:  ['FV_30', 'FV_11'], # Sunday 28.06
        8:  ['FV_32','FV_29', 'FV_33', 'FV_25'],   # Monday 29.06 
        9:  ['FV_30', 'FV_22', 'FV_5', 'FV_4'], # Tuesday 30.06
        10: ['FV_29', 'FV_18', 'FV_3', 'FV_4'], # Wednesday 01.07 'FV_28', 
        11: ['FV_11', 'FV_14', 'FV_7'], # Thursday 02.07
        12: [], # Friday(day-off)
        13: [], # Saturday (day-off)

        # --- 3-d week ---
        14: ['FV_30', 'FV_11'], # Sunday 05.07 
        15: ['FV_32', 'FV_29', 'FV_33', 'FV_19'], # Monday 06.07
        16: ['FV_30', 'FV_22', 'FV_28'], # Tuesday 07.07
        17: ['FV_28', 'FV_29', 'FV_18', 'FV_20', 'FV_22', 'FV_6'], # Wednesday 08.07
        18: ['FV_11', 'FV_21'], # Thursday 09.07
        19: [], # Friday 10.07
        20: [], # Saturday 11.07

        # --- 4-d week ---
        21:['FV_30', 'FV_11', 'FV_15'],
        22:['FV_32', 'FV_29', 'FV_33'],
        23:['FV_30', 'FV_28', 'FV_16'],
        24:['FV_28', 'FV_29', 'FV_18', 'FV_5'],
        25:['FV_11', 'FV_14', 'FV_23'],
        26:[],
        27:[]
    }
    
    current_date = datetime.now()
    current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

    point_day = datetime (2026,6,21)
    delta_days = (current_date-point_day).days
    day_according_schedule = delta_days % 28 # remainder is a day in the schedule_checking
 
    target_date_str = current_date.strftime('%Y%m%d')

    ftp_hook = FTPHook(ftp_conn_id="ftp_default")
    remote_file_path = ftp_hook.list_directory('/data/input/')

    prefix_fv = schedule_checking.get(day_according_schedule,[])
    if not prefix_fv:
        logging.info(f'The day of checking according schedule is {day_according_schedule} since day-off checking is not existed')
        ti.xcom_push(key = 'error in download', value = [])
        return

    prefix_file =tuple([f'{p}_{target_date_str}' for p in prefix_fv])
    logging.info(f'Files for downloaded list: {prefix_file}')

    download_files= []
    error_list = []

    for prefix in prefix_file:
        target_files_name = [ file for file in remote_file_path if prefix in file] #find file day before
        if not target_files_name:
            pure_prefix = prefix.rsplit('_', 1)[0] #cutting prefix if the day before file prefix is absent in FTP
            day_bfore_yesterday = (current_date - timedelta(days=1)).strftime('%Y%m%d')#day before yesterday
            alt_prefix = f"{pure_prefix}_{day_bfore_yesterday}"
            logging.info(f"File with prefix {prefix} not found. Trying alternative: {alt_prefix}")
            target_files_name = [file for file in remote_file_path if alt_prefix in file]
        if not target_files_name:
            msg = f"File {prefix} is missing on FTP"
            logging.warning(msg)
            error_list.append(msg)

    #download_files = []
        for file in target_files_name:
            full_remote_path = f'/AgodaAir/prod/{file}'
            if not ftp_hook.conn:
                ftp_hook = FTPHook(ftp_conn_id="ftp_default")
                
            temp_file = NamedTemporaryFile(suffix=f'_{ds_nodash}.csv', delete=False)
            temp_path = temp_file.name
            temp_file.close ()
            try:
                ftp_hook.retrieve_file(full_remote_path, temp_path)
            except Exception as e:
                logging.warning(f"Connection lost. Retrying file {file} due to: {e}")
                ftp_hook = FTPHook(ftp_conn_id="ftp_default")
                ftp_hook.retrieve_file(full_remote_path, temp_path)

            file_size = os.path.getsize(temp_path)  
            logging.info(f"{file} size={file_size} bytes")

            if file_size==0:
                msg = f"File {prefix_file} is 0 bytes"
                logging.error(msg)
                error_list.append(msg)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                continue

            download_files.append({'original_name':file, 'temp_path': temp_path})
            logging.info("File downloaded successfully")
    ti.xcom_push (key = 'download_errors', value = error_list)    
    return download_files


def check_data(ti):
    received_csv_list = ti.xcom_pull(task_ids='download_file', key='return_value')
    df_airport_codes= pd.read_csv('dags/Airports_code.csv')
    
    columns_to_check_OW = ['observation_date', 'observation_time', 'pos', 'origin', 'destination', 'is_one_way', 'outbound_carrier', 
            'outbound_flight_no', 'outbound_departure_date', 'outbound_departure_time', 'outbound_arrival_date', 'outbound_arrival_time', 'outbound_fare_basis', 'outbound_booking_class',
            'currency', 'sourceota', 'price_outbound', 'price_outbound_usd','is_tax_inc_outin', 'search_class', 'outbound_flight_duration',
            'srp_rank', 'screenshot','platform'] #'route_type', 

    columns_to_check_RT = ['observation_date', 'observation_time', 'pos', 'origin', 'destination', 'is_one_way', 'outbound_carrier', 'inbound_carrier', 
            'outbound_flight_no', 'inbound_flight_no', 'outbound_departure_date', 'outbound_departure_time', 'outbound_arrival_date', 'outbound_arrival_time', 'inbound_departure_date', 
            'inbound_departure_time', 'inbound_arrival_date', 'inbound_arrival_time', 'outbound_fare_basis', 'inbound_fare_basis', 'outbound_booking_class', 'inbound_booking_class',
            'currency', 'sourceota', 'price_outbound', 'price_outbound_usd', 'price_inbound', 'price_inbound_usd', 'is_tax_inc_outin', 'search_class', 'outbound_flight_duration', 'inbound_flight_duration',
            'srp_rank', 'screenshot', 'platform']
    def route_type_check (df_check, df_airport_codes):
        df_check.columns=df_check.columns.str.lower()
        df_airport_codes.columns=df_airport_codes.columns.str.lower()
        #rename columns for original
        ref_df_airport_codes_original = df_airport_codes[['country', 'code']].copy().rename(columns={'country': 'origin_country','code': 'origin_code'})
        #merge Original
        df_check = pd.merge(df_check, ref_df_airport_codes_original, left_on='origin', right_on='origin_code', how='left')
        #rename columns for destination
        ref_df_airport_codes_destination = df_airport_codes[['country', 'code']].copy().rename(columns={'country': 'destination_country','code': 'destination_code'})
        #merge Destination
        df_check = pd.merge(df_check, ref_df_airport_codes_destination, left_on ='destination', right_on ='destination_code', how='left')
        #Value Nan
        df_check.fillna({'origin_country':'Unknown_code','destination_country':'Unknown_code'}, inplace=True)
        #Exatracted route type
        is_domestic_condition = (df_check['origin_country'] == df_check['destination_country']) & (df_check['origin_country']!= 'Unknown_code')
        df_check['Route type should be extracted'] = np.where(is_domestic_condition, 'Domestic', 'International')
        code_unknown_code_condition = (df_check ['origin_country'] == 'Unknown_code') | (df_check ['destination_country'] =='Unknown_code')
        #Empty value in the route type column
        route_type_not_extracted_condition = (df_check['rout_type'].isna()) | (df_check['rout_type'] == '')
        #Incorrect type condition
        incorrect_type_condition = (df_check['Route type should be extracted'] != df_check['rout_type'])
    
        df_check['Route_Check_Status'] = np.select(
        [code_unknown_code_condition, route_type_not_extracted_condition, incorrect_type_condition], 
        ['Unknown code of the country','Route type was not extracted','Route type was extracted incorrectly'],
        default='1'
    )
    
        #removing extra columns created before
        df_check.drop(['origin_country', 'origin_code', 'destination_country', 'destination_code'], axis=1, inplace=True)
        return df_check
    # return df_check
    def empty_value(df_check, colum_to_check_OW, column_to_check_RT):
        def classify(row):
            comments = []
            if row['Route_Check_Status'] != '1':
                comments.append(row['Route_Check_Status'])
            search_class_value = str(row['search_class']).strip().lower()
            if search_class_value != 'economy':
                comments.append(f"Search class is'{search_class_value}'instead of 'Economy'")
            is_one_way_value = str(row['is_one_way']).strip().lower()
            if is_one_way_value in ['yes', 'true']:
                for col in colum_to_check_OW:
                    value = row [col]
                    if pd.isna(value):
                        comments.append(f'Value in {col.capitalize()} was not extracted')
                    elif isinstance(value, str) and value.strip()=='':
                        comments.append(f'Value in {col.capitalize()} was not extracted')
                    elif col != 'is_tax_inc_outin' and isinstance (value, (int, float, str)) and value == 0:
                        comments.append(f'Value in {col.capitalize()} was extracted as 0')
                    elif col != 'is_tax_inc_outin' and isinstance (value, str) and value.strip() in ['0', '0.0']:
                        comments.append(f'Value as 0 was extracted for {col.capitalize()}')
                price_out = row['price_outbound']
                
                if comments:
                    return 0, ';'.join(comments)
                else:
                    return np.nan, ''
            if is_one_way_value in ['no', 'false']:
                for col in column_to_check_RT:
                    value = row [col]
                    if pd.isna(value):
                        comments.append(f'Value in {col.capitalize()} was not extracted')
                    elif isinstance(value, str) and value.strip()=='':
                        comments.append(f'Value in {col.capitalize()} was not extracted')
                    elif col != 'is_tax_inc_outin' and isinstance (value, (int, float, str)) and value == 0:
                        comments.append(f'Value in {col.capitalize()} was extracted as 0')
                    elif col != 'is_tax_inc_outin' and isinstance (value, str) and value.strip() in ['0', '0.0']:
                        comments.append(f'Value as 0 was extracted for {col.capitalize()}')
                    price_out = row['price_outbound']
                price_in = row['price_inbound']
                if pd.notna(price_out) and price_out!='' and pd.notna(price_in) and price_in!='':
                    #exclude_strange_low_prices
                    total_price=float(price_in)+float(price_out)
                    if row['currency'] not in ['AUD','SGD'] and  total_price<100:
                        comments.append(f'Pay attention to the total price of flight(is lower than usually)')
                if comments:
                    return 0, ';'.join(comments)
                else:
                    return np.nan, ''
        df_check[['0/1', 'comments']] = df_check.apply(lambda row: pd.Series(classify(row)), axis = 1)
        df_check['0/1'] = df_check['0/1'].astype(pd.Int64Dtype())
        #new_order_of_columns
        front_col = ['0/1', 'comments']
        df_check = df_check[front_col +[c for c in df_check.columns if c not in front_col]]
        return df_check
    dfs_list = []
    for file_info in received_csv_list:
        file_path = file_info['temp_path']
        original_name = file_info['original_name']

        df_check = pd.read_csv(file_path, encoding='utf-8', sep = ',', dtype={45: str, 46: str})

        df_check = route_type_check(df_check, df_airport_codes)
        df_check = empty_value(df_check, columns_to_check_OW, columns_to_check_RT)
        df_check.drop(columns=['Route_Check_Status', 'Route type should be extracted'], inplace=True)

        file_name = original_name.replace('.csv', '_checked.csv')
    
        checked_file = NamedTemporaryFile(suffix='_checked.csv', delete=False)
        checked_file.close()
        df_check.to_csv(checked_file.name, index=False, encoding='utf-8')
        dfs_list.append({'slack_filename': file_name, 'temp_path': checked_file.name})
        if os.path.exists(file_path):
            os.remove (file_path)

        
    logging.info("Files were checked successfully")
    return dfs_list
    

def slack(ti):
    download_errors = ti.xcom_pull(task_ids = 'download_file', key = 'download_errors')

    checked_files = ti.xcom_pull(task_ids = 'check_data', key ='return_value')

    headers = {'Authorization': f'Bearer {user_token}'}

    if download_errors:
        url_text = 'https://slack.com/api/chat.postMessage'
        error_text = 'The issues that were found during downloading:\n' + '\n'.join(download_errors)
        try:
            response = requests.post(url_text, headers=headers, json={'channel': channel_id, 'text': error_text})
            result_json = response.json()
            if result_json.get('ok'):
                logging.info('Error alert successfully sent to Slack')
            else:
                logging.error(f"Slack API rejected error message: {result_json.get('error')}")
        except Exception as e:
            logging.error(f'Failed to send error alert to Slack: {e}')

    if not checked_files:
        logging.info('files are missing for downloaded in Slack channel')
        return
    
    for file_info in checked_files:
        file_path = file_info['temp_path']
        file_name = file_info['slack_filename']
        if not os.path.exists(file_path):
            logging.info (f'file is absent: {file_path}')
            continue
        
        file_size = os.path.getsize(file_path)
        
        try:
            #method for generation uniq link for current file
            url_step_1 = "https://slack.com/api/files.getUploadURLExternal"
            payload_step_1 = {'filename': file_name, 'length':file_size}

            #request to slack
            result = requests.get(url_step_1, headers=headers, params=payload_step_1).json()
            if not result.get('ok'):
                logging.error (f"Error{file_name}:{result.get('error')}")
            
            upload_url_slack = result.get('upload_url')
            file_id = result.get ('file_id')

            #manual downloading binary file on the get path
            with open (file_path, 'rb') as f:
                result_2 = requests.post(upload_url_slack, files = {'file':f})
                if result_2.status_code !=200:
                    logging.error (f"Error: unsuccessfull downloaded file in the Slack")
                    continue
            url_step_2 = 'https://slack.com/api/files.completeUploadExternal'
            payload_step_2 = {
                "files": json.dumps([{"id": file_id, "title": file_name}]),
                'channel_id': channel_id,
                'initial_comment': f'New checked report: *{file_name}*'
            }
            result_2 = requests.post(url_step_2, headers=headers, data = payload_step_2).json()
            if result_2.get ('ok'):
                logging.info (f'The file {file_name} was successfully downloaded to the channel Slack')
                if os.path.exists(file_path):
                    os.remove(file_path)
            else:
                logging.error (f'Error of doanloaded to the Slack file')
        except Exception as e:
            logging.error(f"Critical error{file_name}: {e}")

    logging.info("File were processed successfully")


with DAG(
    dag_id='airflow_data_quality_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 6, 22),
    schedule='30 6 * * *',
    #schedule = '@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(
        task_id='download_file',
        python_callable=download_file
    )

    task2 = PythonOperator(
        task_id='check_data',
        python_callable=check_data
    )

    task3 = PythonOperator(
        task_id='slack',
        python_callable=slack
    )

    task1 >> task2 >> task3 
    