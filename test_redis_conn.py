import sys
sys.path.append('./')
import redis
import pandas as pd
from datetime import datetime, timedelta

def make_dict(item):
    k = ''
    d = dict()
    for i,v in enumerate(item):
        if i % 2 == 0:
            k = v
        else:
            d[k] = str(""+v)
    return d

def nstr(s):
    if s is None:
        return ''
    s = s.replace('\'', '')
    s = s.replace('\\', '')
    s = s.replace('"', '')
    return str(s)

def to_prm(d):
    properties = dict(
        mmsi=int(d['mmsi']),
        imo=int(d['imo']) if d['imo'] else 0,
        vessel_name=nstr(d['vessel_name']),
        callsign=nstr(d['callsign']),
        vessel_type=nstr(d['vessel_type']),
        vessel_type_code=nstr(d['vessel_type_code']),
        vessel_type_cargo=nstr(d['vessel_type_cargo']),
        vessel_class=nstr(d['vessel_class']),
        length=int(d['length']) if d['length'] else 0,
        width=int(d['width']) if d['width'] else 0,
        flag_country=nstr(d['flag_country']),
        flag_code=int(d['flag_code']) if d['flag_code'] else 0,
        destination=nstr(d['destination']),
        eta=str(""+d['eta']),
        draught=float(d['draught']) if d['draught'] else 0,
        longitude=float(d['longitude']) if d['longitude'] else 0,
        latitude=float(d['latitude']) if d['latitude'] else 0,
        sog=float(d['sog']) if d['sog'] else 0,
        cog=float(d['cog']) if d['cog'] else 0,
        rot=float(d['rot']) if d['rot'] else 0,
        heading=float(d['heading']) if d['heading'] else 0,
        nav_status=nstr(d['nav_status']),
        nav_status_code=int(d['nav_status_code']) if d['nav_status_code'] else 0,
        source=nstr(d['source']),
        ts_pos_utc=nstr(d['ts_pos_utc']),
        ts_static_utc=nstr(d['ts_static_utc']),
        ts_insert_utc=nstr(d['ts_insert_utc']),
        dt_pos_utc=nstr(d['dt_pos_utc']),
        dt_static_utc=nstr(d['dt_static_utc']),
        dt_insert_utc=nstr(d['dt_insert_utc']),
        vessel_type_main=nstr(d['vessel_type_main']),
        vessel_type_sub=nstr(d['vessel_type_sub']),
        message_type=int(d['message_type']) if d['message_type'] else 0,
        eeid=float(d['eeid'])
    )
    return properties

R = redis.Redis(host="34.64.244.41", password="interim-auth-hyungtae-kim-for-eco-marine", db=0,\
                    decode_responses=True)
records = R.execute_command("FT.SEARCH idx:lvi:051207f817ccaa25 * LIMIT 0 100000")
R.close()
records = records[2::2]
exit()
now = datetime.now()
nowDatetime1 = now.strftime('%Y%m%d%H%M%S')
before_one_day = now - timedelta(days=8)
nowDatetime2 = before_one_day.strftime('%Y%m%d%H%M%S')

titems = []
for v in records:
    _d = make_dict(v)
    if _d['mmsi'] == '':
        continue

    if int(_d['ts_pos_utc']) < int(nowDatetime2):
        continue

    titems.append(to_prm(_d))

new = pd.DataFrame.from_dict(titems)
new.to_csv(f"/Users/jeongtaegun/Desktop/redis_scheduler/storage/{nowDatetime1}_lvi_prm.csv", mode='w', index=False, encoding='latin-1')

#df.to_csv(f"{nowDatetime}_lvi_prm.csv", index=False, encoding='latin-1')


#R = redis.Redis(host="34.64.93.191", password="interim-auth-hyungtae-kim-for-eco-marine", db=0, decode_responses=True)
#count, *stream = R.execute_command("FT.SEARCH idx:lvi:ecomarine-master-token-5000 * LIMIT 0 400000")
#R.close()
