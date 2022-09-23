import os
from zipfile import ZipFile
from tqdm import tqdm
import paramiko
import pandas as pd
import numpy as np
import psycopg2
from pymongo import MongoClient, errors
import datetime
from time import time
from multiprocessing import Pool
import logging
import warnings

warnings.filterwarnings(action='ignore')

# ftp 연동
HOST = "13.124.229.2"
USERNAME = "ubuntu"
FILEPATH = "/home/ubuntu/data/oceanlook/zip/"
# PEM = "/Users/jeongtaegun/Desktop/aws_mongodb/pemKey/MarineEco.pem"
# LOCALPATH = "/Users/jeongtaegun/Desktop/aws_mongodb/ftp_file_test/"
# OUT = "/Users/jeongtaegun/Desktop/aws_mongodb/csv_test/"

PEM = "/root/ecomarine/Codes/pemKey/MarineEco.pem"
LOCALPATH = "/root/ecomarine/Codes/ftp_file"


# mongodb 연동
_id = "superadmin"
password = "Ecomarine1!"
host = "13.124.229.2"
port = 27017
db_name = "ecomarine"
URL = f"mongodb://{_id}:{password}@{host}:{port}/{db_name}?authSource=admin"

#postgresql 연동
APP_DB_HOST = "118.67.128.207"
APP_DB_NAME = "oceanlook"
APP_DB_USER = "ecolabmaster"
APP_DB_PASSWORD = "Ecomarine1!"
APP_DB_PORT = 5432


def mongodb_conn():
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(URL)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client


def set_many_mongo_data_to(collection_name, items):
    # collection_name.insert_one(items)
    collection_name.insert_many(items)


def extract_aws_ftp_file():
    file_lists = os.listdir(LOCALPATH)
    # print(file_lists)
    # exit()
    for file in file_lists:
        filePath = os.path.join(LOCALPATH, file)
        if os.path.exists(filePath):
            os.remove(filePath)

    now = datetime.datetime.now()
    nowDateHour = now.strftime('%Y%m%d%H')
    nowTime = now.strftime('%M%S')

    if int(nowTime) > 4000:
        filename = "eco_" + nowDateHour + "3000.zip"
    else:
        filename = "eco_" + nowDateHour + "0000.zip"

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USERNAME, key_filename=PEM)

    sftp = ssh.open_sftp()

    sftp.get(FILEPATH + filename, LOCALPATH + filename)

    sftp.close()
    ssh.close()
    print('File is loaded!!!')

    with ZipFile(LOCALPATH + filename, 'r') as fz:
        fz.printdir()
        fz.extractall(LOCALPATH)

    return LOCALPATH


def make_data(new):

    lsts = []
    tableName = "test_collection"
    for i in tqdm(range(new.shape[0])):

        identify = pd.isna(new['imo'][i])
        if identify:
            new["imo"][i] = int(0)

        dateTime = new["dt_insert_utc"][i].replace("-", "/")
        dateTime = dateTime[8:10] + '/' + dateTime[5:7] + '/' + dateTime[2:4] + \
                   dateTime[10:]
        formatDate = "%d/%m/%y %H:%M:%S"
        dateTime = datetime.datetime.strptime(dateTime, formatDate)

        dicts = \
            {

                "datetime": dateTime,
                "metaData":
                    {
                        "mmsi": int(new["mmsi"][i]),
                        "imo": new["imo"][i],
                        "ts_pos_utc": str(new["ts_pos_utc"][i]),
                        "dest_code": "",
                        "geometry":
                            {
                                "type": "Point",
                                "coordinate":
                                    [
                                        float(new["longitude"][i]),
                                        float(new["latitude"][i])
                                    ]
                            }
                    },
                "properties":
                    {
                        "mmsi": int(new["mmsi"][i]),
                        "imo": new["imo"][i],
                        "vessel_name": str(new["vessel_name"][i]),
                        "callsign": str(new["callsign"][i]),
                        "vessel_type": str(new["vessel_type"][i]),
                        "vessel_type_code": int(new["vessel_type_code"][i]),
                        "veseel_type_cargo": str(new["vessel_type_cargo"][i]),
                        "vessel_class": str(new["vessel_class"][i]),
                        "length": str(new["length"][i]),
                        "width": str(new["width"][i]),
                        "flag_country": str(new["flag_country"][i]),
                        "flag_code": int(new["flag_code"][i]),
                        "destination": str(new["destination"][i]),
                        "eta": str(new["eta"][i]),
                        "draught": float(new["draught"][i]),
                        "longitude": float(new["longitude"][i]),
                        "latitude": float(new["latitude"][i]),
                        "sog": float(new["sog"][i]),
                        "cog": float(new["cog"][i]),
                        "rot": float(new["rot"][i]),
                        "heading": int(new["heading"][i]),
                        "nav_status": str(new["nav_status"][i]),
                        "nav_status_code": int(new["nav_status_code"][i]),
                        "source": str(new["source"][i]),
                        "ts_pos_utc": str(new["ts_pos_utc"][i]),
                        "ts_static_utc": str(new["ts_static_utc"][i]),
                        "ts_insert_utc": str(new["ts_insert_utc"][i]),
                        "dt_pos_utc": str(new["dt_pos_utc"][i]),
                        "dt_static_utc": str(new["dt_static_utc"][i]),
                        "dt_insert_utc": str(new["dt_insert_utc"][i]),
                        "vessel_type_main": str(new["vessel_type_main"][i]),
                        "vessel_type_sub": str(new["vessel_type_sub"][i]),
                        "message_type": int(new["message_type"][i]),
                        "eeid": str(new["eeid"][i]),
                        "dest_code": "",
                        "dept_code": "",
                        "etd": "",
                        "geometry":
                            {
                                "type": "Point",
                                "coordinates":
                                    [
                                        float(new["longitude"][i]),
                                        float(new["latitude"][i])
                                    ]
                            }
                    }

            }

        lsts.append(dicts)


    client = mongodb_conn()
    myDB = client[db_name]
    myCollection = myDB[tableName]
    myCollection.insert_many(lsts)
    print("insert completed")
    client.close()


def custom_mmsi():

    try:

        conn = psycopg2.connect(host=APP_DB_HOST, dbname=APP_DB_NAME, user=APP_DB_USER,\
                            password=APP_DB_PASSWORD,port=APP_DB_PORT)
        query = "SELECT emv_mmsi FROM em_vessel"
        cursor = conn.cursor()
        cursor.execute(query)
        ship_mmsis = cursor.fetchall()
    except:
        print("Connection Error")

    finally:
        conn.commit()
        cursor.close()

    return ship_mmsis

if __name__ == "__main__":

    """
    ship_mmsis = custom_mmsi()
    ship_mmsis = list(set(ship_mmsis))
    df = pd.DataFrame(columns=['mmsi'])
    for i in range(len(ship_mmsis)):
        df.loc[i] = ship_mmsis[i]

    df.to_csv("ship_customer_mmsi.csv", index=False)
    exit()
    """

    start = time()
    '''
    now = datetime.datetime.now()
    nowMonthDate= now.strftime('%Y%m')
    tableName = "ts_lvi_prm-" + nowMonthDate
    # print(tableName)
    # exit()
    tableName = "test_collection"
    path = "./zip/"
    file_lists = os.listdir(path)
    out = "./csv/"
    print(file_lists)

    for filename in file_lists:

        with ZipFile(path + filename, 'r') as fz:
            fz.printdir()
            fz.extractall(out)

    exit()
    '''
    out = "./csv/"
    file_lists = os.listdir(out)


    lsts = []

    for fileName in tqdm(file_lists):
        if 'vessel' in fileName:
            vessel_file = os.path.join(out, fileName)
            df = pd.read_csv(vessel_file, sep=',', encoding='latin-1')
            lsts.append(df)
        if 'mmsi' in fileName:
            mmsi_file = os.path.join(out, fileName)
            df = pd.read_csv(mmsi_file, sep=',', encoding='latin-1')
            lsts.append(df)

        if len(lsts) > 1:
            new = pd.concat(lsts)

        else:
            new = lsts[0]
        new['eta'] = new['eta'].astype(str)

    new = new.drop(['FID', 'position'], axis=1)
    
    ship_mmsis = custom_mmsi()
    # print(len(ship_mmsis[0][0]))
    # exit()
    ship_mmsis = [d for d in ship_mmsis if len(d[0]) > 5]
    ship_mmsis = list(set(ship_mmsis))
    df = pd.DataFrame(columns=new.keys())
    idx = 0
    for sm in ship_mmsis:

        sm = int(sm[0])

        if sm in list(new['mmsi'].astype(int)):
            jdx = list(new['mmsi']).index(sm)
            df.loc[idx] = new.loc[jdx]
            idx += 1
    data = make_data(df)
    num = 10000
    qtn = len(data) // num
    rdr = len(data) % num

    arrays = []
    for i in range(qtn):
        tmp = new[i * num: (i + 1) * num]
        tmp.reset_index(drop=False, inplace=True)

        for j in tqdm(range(tmp.shape[0])):
            tmp['eta'][j] = str(tmp['eta'][j]).zfill(8)

        arrays.append(tmp)

    tmp = new[(i + 1) * num:]
    tmp.reset_index(drop=False, inplace=True)

    for i in tqdm(range(tmp.shape[0])):
        tmp['eta'][i] = str(tmp['eta'][i]).zfill(8)

    arrays.append(tmp)
    cnt = os.cpu_count()
    pool = Pool(cnt)
    pool.map(make_data, arrays)
    pool.close()
    pool.join()

    end = time()
    print("elapsed time: {}".format(end - start))

