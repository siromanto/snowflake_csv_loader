#!/usr/bin/env python
import snowflake.connector
from configs import config


def create_connector():
    # Gets the version
    ctx = snowflake.connector.connect(
        user=config.DB_USERNAME,
        password=config.DB_PASSWORD,
        account=config.DB_ACCOUNT
        )
    return ctx

def run():
    ctx = create_connector()
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

        print(f'Database LINKEDIN_AD_PERFORMANCE_TRAFFICBYDAY successfully created or cleared')
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    run()