import os
import pandas as pd
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.sql import text
from multiprocessing import Pool



HOST = "118.67.128.207"
DATABASE= "oceanlook"
USERNAME = "ecolabmaster"
PASSWORD = "Ecomarine1!"
PORT = 5432
SCHEMA = 'public'
TABLENAME = "lvi_prm_2022"

CSVNAME = "220707-56척+14척 선박리스트.xlsx"

url = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
engine = sqlalchemy.create_engine(url)



sql = (
        f"""
            SELECT tablename FROM pg_tables
            WHERE tablename LIKE '{TABLENAME}%'
    
        """)

with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))

total_tables = query.fetchall()

def extract_data(mmsi):

    lsts = []
    for i in tqdm(range(len(total_tables))):
        sql = (
            f"""
                    SELECT * FROM {total_tables[i][0]}
                    WHERE mmsi = {mmsi};

                """)
        with engine.connect().execution_options(autocommit=True) as conn:
            query = conn.execute(text(sql))

        df = pd.DataFrame(query.fetchall())
        try:
            df = df.drop(['geom'], axis=1)
        except:
            print("mmsi: ", mmsi)
        lsts.append(df)

    total_df = pd.concat(lsts)
    total_df.to_csv(f"56_data/56_mmsi_ship_data_{mmsi}.csv", index=False, sep=',', encoding='utf-8')

if __name__ == "__main__":

    df_mmsi = pd.read_excel(CSVNAME, sheet_name='Sheet1')
    df_mmsi = df_mmsi.loc[2:57, 'Unnamed: 2']
    lsts_mmsi = list(df_mmsi)

    cnt = os.cpu_count()
    pool = Pool(cnt)
    pool.map(extract_data, lsts_mmsi)
    pool.close()
    pool.join()