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
CHUNKS_VALUE = 4


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

                self.transform_csv_fields(df, case=report_name)

                df.to_csv(f'{os.getcwd()}/data/to_load_{report_name}/to_load_{file}', index=False, encoding='utf-8')

                print(f'DONE === {file}')
        print('DONE')

    def transform_csv_fields(self, df, case):
        if case == 'campaign_performance':
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
        elif case == 'ad_performance':
            self.check_df_params(df, 'START_DATE_IN_UTC', case='TRANSFORM_DATA')
            self.check_df_params(df, 'CLICK_THROUGH_RATE', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'ENGAGEMENT_RATE', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'OPEN_RATE', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'CLICK_THROUGH_RATE_SPONSORED_INMAIL', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'CONVERSION_RATE', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'LEAD_FORM_COMPLETION_RATE', case='TRANSFORM_PERCENTS')
            self.check_df_params(df, 'TOTAL_BUDGET', case='TRANSFORM_FLOAT')
            self.check_df_params(df, 'AD_INTRODUCTION_TEXT', case='TRANSFORM_QUOTES')
            self.check_df_params(df, 'AD_HEADLINE', case='TRANSFORM_QUOTES')
            self.check_df_params(df, 'DSC_NAME', case='TRANSFORM_QUOTES')

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
            elif case == 'TRANSFORM_QUOTES':
                df[param] = df[param].apply(lambda x: x.replace("’", '').replace("'", '') if isinstance(x, str) else x)

    def load_data(self, report_name, table_name):
        total_rows_count = 0

        for file in os.listdir(f'{os.getcwd()}/data/to_load_{report_name}'):
            if file.endswith('.csv'):
                print(f'Try to load file: {file} ===>')
                print(f'Copy into table {table_name}...')
                rows = 0

                df = pd.read_csv(f'{os.getcwd()}/data/to_load_{report_name}/{file}')
                df = df.fillna(0)
                # df = df.where(pd.notnull(df), '')

                heads = ','.join(list(df))

                print("Start uploading data....")
                for i in range(len(df.index)):
                    row = """,""".join("'{0}'".format(str(x)) for x in list(df.iloc[i]))
                    print(f'Write row --- {i+1}, DATE --- {df["START_DATE_IN_UTC"][i]}, COMPANY --- {df["CAMPAIGN_NAME"][i]}')

                    try:
                        self.conn.cursor().execute(
                            "INSERT INTO {}({}) "
                            "VALUES ({})".format(table_name, heads, row))
                        rows += 1

                    except Exception as e:
                        self.conn.rollback()
                        raise e

                print(f'Finish with file --- {file}, ROWS uploaded --- {rows}...')
                total_rows_count += rows
        self.conn.cursor().close()
        self.conn.close()
        print(f"Data imported successfully, total rows load --- {total_rows_count}")

    def _cleanup_data(self, table_name):
        self.conn.cursor().execute('DELETE FROM {}'.format(table_name))

    def _execute_queries_for_upload(self, report_path, storage_path, table_name):
        self.conn.cursor().execute('PUT \'file://{}\' \'{}\''.format(report_path, storage_path))
        self.conn.cursor().execute('COPY INTO {} FROM \'{}\' '
                                   'FILE_FORMAT=(SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY=\'"\' ERROR_ON_COLUMN_COUNT_MISMATCH=false)'.format(table_name, storage_path))
        self.conn.cursor().execute('REMOVE \'{}\''.format(storage_path))

    def load_raw_data_from_csv(self, report_name, table_name):

        for file in os.listdir(f'{os.getcwd()}/data/to_load_{report_name}'):
            if file.endswith('.csv'):
                print(f'Try to load file: {file} ===>')

                file_path = f"{os.getcwd()}/data/to_load_{report_name}/{file}"

                curr = self.conn.cursor()
                storage_path = '@%{}/{}'.format(table_name, file)

                try:
                    curr.execute('BEGIN')
                    # self._cleanup_data(curr, table_name)
                    self._execute_queries_for_upload(file_path, storage_path, table_name)
                    curr.execute('COMMIT')

                    print(f'Finish with file --- {file}...')
                except Exception as e:
                    print(e)

        self.conn.cursor().close()
        self.conn.close()
        print(f"Data imported successfully")



    def load_data_by_chunks(self, report_name, table_name):
        total_rows_count = 0

        for file in os.listdir(f'{os.getcwd()}/data/to_load_{report_name}'):



            if file.endswith('.csv'):
                print(f'Try to load file: {file} ===>')
                print(f'Copy into table {table_name}...')

                df = pd.read_csv(f'{os.getcwd()}/data/to_load_{report_name}/{file}')
                df = df.fillna(0)
                heads = ','.join(list(df))

                print("Start uploading data....")
                file_rows = 0

                for part in helpers.get_data_by_chunks(range(len(df.index)), CHUNKS_VALUE):
                    rows_list = [str(list(df.iloc[x])).replace("[", "(").replace("]", ")") for x in part]
                    query = """,""".join(rows_list)

                    try:
                        self.conn.cursor().execute(
                            "INSERT INTO {}({}) "
                            "VALUES {}".format(table_name, heads, query))

                        file_rows += CHUNKS_VALUE

                    except Exception as e:
                        self.conn.rollback()
                        raise e

                    print(f"CHUNK UPLOADED, file rows --- {file_rows}")

                total_rows_count += file_rows

        self.conn.cursor().close()
        self.conn.close()
        print(f"Data imported successfully, all rows --- total_rows_count")

    def run(self, report_name='data'):
        self.get_csvs(report_name)

    def load(self, report_name='data', table_name=None):
        # self.load_data_by_chunks(report_name, table_name)
        self.load_raw_data_from_csv(report_name, table_name)


if __name__ == '__main__':
    # Transform(config).run(report_name='campaign_performance')
    # Transform(config).load(report_name='campaign_performance', table_name='LINKEDIN_CAMPAIGN_PERFORMANCE_TRAFFICBYDAY')

    # Transform(config).run(report_name='ad_performance')
    # Transform(config).load(report_name='ad_performance', table_name='LINKEDIN_TEST_TABLE')
    # Transform(config).load(report_name='ad_performance', table_name='LINKEDIN_AD_PERFORMANCE_TRAFFICBYDAY')

    # Transform(config).load_raw_data_from_csv(report_name='ad_performance', table_name='LINKEDIN_TEST_TABLE')
    Transform(config).load_raw_data_from_csv(report_name='ad_performance', table_name='LINKEDIN_AD_PERFORMANCE_TRAFFICBYDAY')

