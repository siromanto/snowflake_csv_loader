#!/usr/bin/env python
import snowflake.connector
from configs import config

# Gets the version
ctx = snowflake.connector.connect(
    user=config.DB_USERNAME,
    password=config.DB_PASSWORD,
    account=config.DB_ACCOUNT
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])

    cs.execute('USE WAREHOUSE {}'.format(config.WAREHOUSE))
    cs.execute('USE DATABASE {}'.format(config.DATABASE))

    cs.execute(
        "CREATE OR REPLACE TABLE "
        "LINKEDIN_AD_PERFORMANCE_TRAFFICBYDAY({})".format(config.COMPAIN_PERFORMANCE_DB_COLUMNS))
finally:
    cs.close()
ctx.close()