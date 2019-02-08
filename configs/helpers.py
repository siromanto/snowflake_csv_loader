import json, os
from datetime import timedelta, datetime
from . import config

import snowflake.connector as connector



DATETIME_FORMAT = '%m/%d/%Y'


def normalize_backfill_start_end_time(start_date, end_date):
    end_time = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime(DATETIME_FORMAT)
    start_time = start_date.strftime(DATETIME_FORMAT)
    return start_time, end_time


# def get_client_config(client_name, conf_path):
#     configfile = get_resource_path()[0]
#     with open(configfile, 'r') as f:
#         conf = json.load(f).get(client_name)
#     assert conf is not None
#     return conf


def establish_db_conn(user, password, account, db, warehouse):
    conn = connector.connect(
        user=user,
        password=password,
        account=account
    )
    conn.cursor().execute('USE DATABASE {}'.format(db))
    conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
    return conn


def perform_db_routines(client_name, sql):
    # configfile = get_resource_path()[0]

    # client_config = get_client_config(client_name, configfile)
    conn = establish_db_conn(config.DB_USERNAME, config.DB_PASSWORD, config.DB_ACCOUNT,
                             config.DB, config.WAREHOUSE)
    conn.autocommit(False)
    curr = conn.cursor()
    queries_list = sql.split(';')
    try:
        curr.execute('BEGIN')
        for q in queries_list:
            curr.execute(q)
        curr.execute('COMMIT')
    finally:
        curr.close()
        conn.close()

# def create_table(name):


