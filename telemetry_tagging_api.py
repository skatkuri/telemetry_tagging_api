import json
import csv
import pandas
from pandas import DataFrame
import requests
import datetime
import collections
from datetime import date, datetime
import snowflake.connector
import http.client
from collections import OrderedDict
from snowflake.connector import connect as sf_connect
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

csv_obj = []

url='https://api-gw.meetflo.com/api/v2/telemetry/tags'
start_date="2019-04-27T00:00:00Z"
end_date="2019-10-27T00:00:00Z"
device_id="c8df8457f649"
con = snowflake.connector.connect(
                user='',
                password='',
                account='gx82091',
                warehouse='dynamo_queries',
                database='dw',
                schema='dynamodb'
            )

#cursor=connection.cursor()
query="SELECT cast(device_id as varchar) as deviceId , 'telemetry' as source, 'sm.implied_away' as tag, 'null' as status,to_char(dateadd(hour, 0, aggregate_date),'yyyy-MM-dd\T\HH:MI:SS\Z') as start_ts, to_char(dateadd(second,-1,dateadd(hour, 24, aggregate_date)),'yyyy-MM-dd\T\HH:MI:SS\Z') as end_ts from dw.telemetry.flo_device_daily where max_gpm = 0 and aggregate_date > cast('2019-08-27' as date)  and device_id ='0c1c577331d5' order by device_id,aggregate_date;"
print(query)
#row_headers=[x[0] for x in cs.description]
cs=con.cursor()
result = cs.execute(query)
rows = cs.fetchall()
print(rows)
objects_list = []
for row in rows:
        d = collections.OrderedDict()
        d['deviceId'] = row[0]
        d['source'] = row[1]
        d['tag'] = row[2]
        d['status'] = row[3]
        d['start'] = row[4]
        d['end'] = row[5]
        objects_list.append(d)
for i in objects_list:
    json_data=json.dumps(i,sort_keys=True)
    print("started loading into API: %s %s ".format(i["deviceId"],i["start"]))
    res=requests.get(url,headers={"Content-Type": "application/json","Authorization": "Bearer token"},
                        params={"deviceId": i["deviceId"]})
    
    print("ended loading into API: %s %s ".format(i["deviceId"]+ print(res)))
print('done')
