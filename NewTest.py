#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta
import traceback
import os
import logging
import sys
import json

from dotenv import load_dotenv
import requests
import pandas as pd
from pandas import json_normalize

from BTFeTL import data_transformation, get_exception


class Historian_Connection:
    """
    Creating a class to store Historian connection information as well as the access token.
    Refactored as a class to avoid using global variables and to keep related code together.
    Can be reused as required.
    """
    username = None
    password = None
    server = None
    url = None
    client_id = None
    client_secret = None
    access_token = None

    def __init__(self, username: str, password: str, server: str, url:str, client_id: str, client_secret: str):
        """
        Initializes a Historian_Connection object with required connecion details
        """
        # Seperating server field from url to make the function generic
        self.username = username
        self.password = password
        self.server = server
        self.url = url.format(server)
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self) -> bool:
        """
        Single call to get Historian access token with resource owner credentails
        in the body and client credentials as the basic auth header.

        Returns:
        :access_token (obj):

        """
        data = {'grant_type': 'password', 'username': self.username, 'password': self.password}
        try:
            response = requests.post(self.url, data=data, allow_redirects=False, auth=(self.client_id, self.client_secret))
            self.access_token = response.json()
            logger.info('Successfully set Access Token\n')
            return False
        except Exception as e:
            logger.error('Unable to set Access Token\n')
            logger.error(e)
            return True


def print_data_to_df(tag_name: str, retrieval_mode: str, cycle_time: str, start_time: str, end_time: str,
                     results_df: pd.DataFrame, time_stamp_populated: int, historian_conn: Historian_Connection) -> pd.DataFrame:
    """
    Function that retirieves data for the target tag and appends to the results dataframe

    Params:
    :tag_name (str):
    :retrieval_mode (str):
    :cycle_time (str):
    :start_time (str):
    :end_time (str):
    :results_df (pd.DataFrame):
    :time_stamp_populated (int):
    :historian_conn (Historian_Connection):

    Returns:
    :results_df (pd.DataFrame):
    """
    # As the "#" symbol gives issues in proficy, we replace it with *
    target_tag_name = tag_name.replace("#", "*")

    if retrieval_mode.lower()=="rawbytime" or retrieval_mode.lower()=="rawwithgaps":
        url = f'https://{historian_conn.server}:443/historian-rest-api/v1/datapoints/raw/{target_tag_name}/{start_time}/{end_time}/0/0'
    elif retrieval_mode.lower()=="lab":
       url = f'https://{historian_conn.server}:443/historian-rest-api/v1/datapoints/sampled?tagNames={target_tag_name}\
                       &start={start_time}&end={end_time}&samplingMode=7&calculationMode=1&direction=0&count=0&intervalMs={str(cycle_time)}'
    else:
       url = f'https://{historian_conn.server}:443/historian-rest-api/v1/datapoints/interpolated/{target_tag_name}/{start_time}/{end_time}/0/{str(cycle_time)}'

    headers = {'Authorization': f"Bearer {historian_conn.access_token['access_token']}"}

    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        data = str(data['Data'][0]['Samples'])
        data = json.loads(data.replace('\'', '"'))
        data = json_normalize(data)

        # Drop bad quality data
        if (len(data) == 0):
            logging.info(f'No data or bad quality data: {tag_name} ')
            results_df[tag_name] = pd.NA
            return results_df
        elif (len(data) != 0 and retrieval_mode.lower() != "lab"):
            data = data[data["Quality"] == 3]
        elif (len(data) != 0 and retrieval_mode.lower() == "lab"):
            data.loc[data['Quality'] == 0, ['Value']] = None

        # Then check if there is still data
        if (len(data) != 0):
            data['TimeStamp'] = pd.to_datetime(data['TimeStamp'], format='%Y-%m-%dT%H:%M:%S')
            data['TimeStamp'] = data['TimeStamp'].apply(lambda x: x.replace(microsecond=0))
            data.drop(columns='Quality', errors='ignore', inplace=True)
            # Remove duplicates due to dowsampling
            data = data[~data.index.duplicated()]

    except Exception as e:
        logger.exception(e)
        traceback.print_exc
        return results_df

    # If I have a variable with gaps, I fill it in and fill the gaps with "NA"
    try:
        # ONUR - is it OK if we have the data in narrow format instead of wide if it's 'rawbytime'? It's gonna take much less space, and also will be easier to post-treat.
        if (retrieval_mode.lower()=="rawbytime"):
            data.reset_index(drop=True, inplace=True) #ONUR
            data.rename(columns={'Value': tag_name}, inplace=True)
            if time_stamp_populated == 0: #ONUR
                time_stamp_populated = 1 #ONUR
                results_df = data #ONUR
            else: #ONUR
                results_df = results_df.merge(data, on='TimeStamp', how='outer')
            logger.info(f'End: {tag_name}') #ONUR
        elif (retrieval_mode.lower()=="lab"):
            # ONUR - to be able to use the timestamp in first column
            results_df['TimeStamp'] = data['TimeStamp']
            results_df[tag_name] = data['Value'] #ONUR
            if time_stamp_populated == 0:
                time_stamp_populated = 1
            logger.info(f'End: {tag_name}')
        else:
            logger.info(f"{tag_name}: Invalid extraction method. Use lab or rawbytime")
        # If I am retrieving with gaps to fill, I postprocess data and fill it
        # The first time I populate the timestamp, I set one.
        return results_df
    except Exception as e:
        logging.exception(e)
        traceback.print_exc()
        return results_df


def get_data_as_df(historian_conn: Historian_Connection, tags_list: list, retrieval_mode: str, cycle_time: str, start_time: str, end_time: str):
    results_df = pd.DataFrame()
    time_stamp_populated = 0
    for tags in tags_list:
        results_df = print_data_to_df(tags, retrieval_mode, cycle_time, start_time, end_time, results_df, time_stamp_populated, historian_conn)
        if len(results_df) > 0:
            time_stamp_populated = 1
    return results_df


if __name__ == '__main__':
    logging.basicConfig(filename='./logs/logs.log',
                        format='%(asctime)s -> [%(levelname)s] %(message)s',
                        filemode='a')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.info('----------------------- STARTED BTF DATA EXTRACTION ----------------------------\n')

    start_time = datetime.now()
    #---------------------------------------------------------------------------------------------------------------

    # GETTING TIME BOUNDARIES
    today = date.today()
    yesterday = today - timedelta(days=1)
    extract_start = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
    extract_end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

    end_time_str = extract_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_time_str = extract_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    logger.info(f'Extraction Start Time: {extract_start}')
    logger.info(f'Extraction End Time: {extract_end}')

    #---------------------------------------------------------------------------------------------------------------
    # GETTING THE ENVIRONMENT VARIABLES AND ACCESS TOKEN
    load_dotenv()
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    username = os.getenv('ion_username')
    password = os.getenv('ion_password')
    server = os.getenv('server')
    url = os.getenv('token_url')
    historian_conn = Historian_Connection(username, password, server, url, client_id, client_secret)
    flag = historian_conn.get_token()
    if flag:
        sys.exit('Unable to access Historian Database')

    #---------------------------------------------------------------------------------------------------------------
    # GETTING TAGS
    tag_data = pd.read_csv('./data/T2 Tags.csv')
    # TODO: What is this for? It's not being used anywhere
    ### Loops for tags on multiple LPDs
    LPD_Numbers = [1,2,3,4,5,6,7]
    # Note, in the future, have this operation done on a list autmatically

    final_data = None

    counter = 0
    for index, row in tag_data.iterrows():
        tank = row['Tank number']
        columns = {row['tIT']:f'Temp. {tank}', row['LIT']:f'Level {tank}', row['Unload Pump']:f'{tank}',
                   row['Density']:f'Density {tank}', row['Kilo']:f'Kilos {tank}', row['GCAS']:f'GCAS {tank}'}

        try:
            # GETTING RAW DATA
            raw_data = get_data_as_df(historian_conn, list(columns.keys()), 'lab', '120000', end_time_str, start_time_str)
            raw_data.drop(columns='index', errors='ignore', inplace=True)
            for column in columns:
                if column in raw_data:
                    raw_data.rename(columns={column: columns[column]}, inplace=True)

            if final_data is None:
                final_data = raw_data
            elif len(raw_data) == 0:
                final_data[raw_data.columns] = pd.NA
            else:
                final_data = final_data.merge(raw_data, how='outer', on='TimeStamp')
            logger.info(f'Tank {tank} data extracted\n')
        except Exception as e:
            logger.exception(f'Tank {tank} could not get raw\n')
            logger.exception(get_exception())

        counter += 1
        print(f'Extract counter: {counter}/{len(tag_data)}\r', end='')

    print('')
    final_data.rename(columns={'TimeStamp': 'Time'}, inplace=True)

    # final_data.to_csv(f'./data/Raw Data {date.today()}.csv', index=False)
    end_time = datetime.now()
    logger.info(f'Time for Data Extraction: {(end_time - start_time).seconds / 60} mins\n')
    logger.info('----------------------- END OF BTF DATA EXTRACTION ----------------------------\n\n')

    # Passing the extracted data to transformation logic
    data_transformation(final_data)
