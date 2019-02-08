# -*- coding: utf-8 -*-

import csv
import os
import pandas as pd
import datetime

from configs import config, helpers


# TRANSFORM_DATA = lambda x: datetime.datetime.strptime(x, helpers.DATETIME_FORMAT)
# TRANSFORM_PERCENTS = lambda x: x.replace('%', '')
# TRANSFORM_FLOAT = lambda x: x.replace(',', '')


INPUT_DATETIME_FORMAT = '%m/%d/%Y'
OUTPUT_DATETIME_FORMAT = "%Y-%m-%d"


class Transform:

    def __init__(self, con_config):
        self.config = con_config
        self.conn = self.create_connection()

    def create_connection(self):
        conn = helpers.establish_db_conn(
            self.config.DB_USERNAME,
            self.config.DB_PASSWORD,
            self.config.DB_ACCOUNT,
            self.config.DATABASE,
            self.config.WAREHOUSE
        )
        return conn

    def get_csvs(self, report_name):
        for file in os.listdir(f'{os.getcwd()}/data/clear_{report_name}'):
            if file.endswith('.csv'):
                print(f'Working with file {file} ===>')
                df = pd.read_csv(f'{os.getcwd()}/data/clear_{report_name}/{file}')

                self.transform_csv_fields(df)

                df.to_csv(f'{os.getcwd()}/data/to_load_{report_name}/to_load_{file}', index=False, encoding='utf-8')

                print(f'DONE === {file}')
        print('DONE')

    def transform_csv_fields(self, df):
        self.check_df_params(df, 'START_DATE_IN_UTC', case='TRANSFORM_DATA')
        self.check_df_params(df, 'CAMPAIGN_GROUP_START_DATE', case='TRANSFORM_DATA')
        self.check_df_params(df, 'CAMPAIGN_GROUP_END_DATE', case='TRANSFORM_DATA')
        self.check_df_params(df, 'CLICK_THROUGH_RATE', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'ENGAGEMENT_RATE', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'OPEN_RATE', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'CLICK_THROUGH_RATE_SPONSORED_INMAIL', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'CONVERSION_RATE', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'LEAD_FORM_COMPLETION_RATE', case='TRANSFORM_PERCENTS')
        self.check_df_params(df, 'TOTAL_BUDGET', case='TRANSFORM_FLOAT')
        self.check_df_params(df, 'CAMPAIGN_GROUP_TOTAL_BUDGET', case='TRANSFORM_FLOAT')

    def check_df_params(self, df, param, case=None):
        if param in df:
            if case == 'TRANSFORM_DATA':
                df[param] = df[param].apply(
                    lambda x: datetime.datetime.strptime(x, INPUT_DATETIME_FORMAT).strftime(OUTPUT_DATETIME_FORMAT) if isinstance(x, str) else '3000-01-01'
                )
            elif case == 'TRANSFORM_PERCENTS':
                df[param] = df[param].apply(lambda x: x.replace('%', '') if isinstance(x, str) else x)
            elif case == 'TRANSFORM_FLOAT':
                df[param] = df[param].apply(lambda x: x.replace(',', '') if isinstance(x, str) else x)

    def load_data(self, report_name, table_name):
        for file in os.listdir(f'{os.getcwd()}/data/to_load_{report_name}'):
            if file.endswith('.csv'):
                print(f'Try to load file: {file} ===>')
                print(f'Copy into table {table_name}...')

                df = pd.read_csv(f'{os.getcwd()}/data/to_load_{report_name}/{file}')
                df = df.fillna(0)
                df = df.where(pd.notnull(df), '')

                heads = ','.join(list(df))

                print("Start uploading data....")
                for i in range(len(df.index)):
                    row = """,""".join("'{0}'".format(str(x)) for x in list(df.iloc[i]))
                    print(f'Write row --- {i+1}, DATE --- {df["START_DATE_IN_UTC"][i]}, COMPANY --- {df["CAMPAIGN_NAME"][i]}')

                    try:
                        self.conn.cursor().execute(
                            "INSERT INTO {}({}) "
                            "VALUES ({})".format(table_name, heads, row))

                    except Exception as e:
                        self.conn.rollback()
                        raise e

                print(f'Finish with file ==> {file}...')
        self.conn.cursor().close()
        self.conn.close()
        print('Data imported successfully')



    def run(self, report_name='data', table_name=None):
        self.get_csvs(report_name)

    def load(self, report_name='data', table_name=None):
        self.load_data(report_name, table_name)

if __name__ == '__main__':
    Transform(config).run(report_name='campaign_performance', table_name='LINKEDIN_CAMPAIGN_PERFORMANCE_TRAFFICBYDAY')
    Transform(config).load(report_name='campaign_performance', table_name='LINKEDIN_CAMPAIGN_PERFORMANCE_TRAFFICBYDAY')
    # Transform(config).run(report_folder='clear_ad_performance')

