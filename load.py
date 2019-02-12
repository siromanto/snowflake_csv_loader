# -*- coding: utf-8 -*-

import os
import pandas as pd

from configs import config, helpers


class Load:

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
                                   'FILE_FORMAT=(SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY=\'"\')'.format(table_name, storage_path))
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

                    print(f'FINISH FILE LOADING...')
                except Exception as e:
                    print(e)

        self.conn.cursor().close()
        self.conn.close()
        print(f"Data imported successfully")

    def load(self, report_name='data', table_name=None):
        # self.load_data_by_chunks(report_name, table_name)
        self.load_raw_data_from_csv(report_name, table_name)


if __name__ == '__main__':
    # Load(config).load(report_name='campaign_performance', table_name='LINKEDIN_CAMPAIGN_PERFORMANCE_TRAFFICBYDAY')
    # Load(config).load(report_name='ad_performance', table_name='LINKEDIN_AD_PERFORMANCE_TRAFFICBYDAY')
    Load(config).load(report_name='aud_network_campaign_performance', table_name='LINKEDIN_AUD_NETWORK_COMPAIGN_PERFORMANCE_TRAFFICBYDAY')
    Load(config).load(report_name='aud_network_ad_performance', table_name='LINKEDIN_AUD_NETWORK_AD_PERFORMANCE_TRAFFICBYDAY')

    # Load(config).load_raw_data_from_csv(report_name='ad_performance', table_name='LINKEDIN_TEST_TABLE')


