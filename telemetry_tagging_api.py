import os
from snowflake_connector.snowflake_db import SnowFlakeDb

# SNOWFLAKE CONNECTION PARAMETERS
sf_user = os.environ["SF_USER"]
sf_pass = os.environ["SF_PASS"]
sf_warehouse = os.environ["SF_WAREHOUSE"]
sf_role = os.environ["SF_ROLE"]
sf_database = os.environ["SF_DATABASE"]
sf_schema = os.environ["SF_SCHEMA"]

sf_db = SnowFlakeDb(sf_user, sf_pass, sf_warehouse, sf_role, sf_database, sf_schema)

query = "SELECT device_id,aggregate_date from dw.telemetry.flo_device_daily where aggregate_date = '2020-04-20' limit 10;"
print(query)

results = sf_db.fetchall(query)
print(results)



