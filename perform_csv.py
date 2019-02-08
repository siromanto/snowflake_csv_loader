# -*- coding: utf-8 -*-

import csv
import os
import pandas as pd
import datetime

from configs import config, helpers


class PerformCsv:
    # for filename in os.listdir(os.getcwd()):

    # def prepare_header_for_new_csv(self, file):
    #     writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
    #     writer.writeheader()
    #     return writer
    def __init__(self, config):
        self.config = config
        # self.conn = self.create_connection()

    def create_connection(self):
        conn = helpers.establish_db_conn(
            self.config.DB_USERNAME,
            self.config.DB_PASSWORD,
            self.config.DB_ACCOUNT,
            self.config.DB,
            self.config.WAREHOUSE
        )
        return conn


    def investigate_csv(self, filename):
        pass


    def parse_columns_names(self, headers):
        return [n.replace('(', '').replace(')', '').replace(" ", "_").replace("-", "_").upper() for n in headers]

    def transform_csvs_to_correct_format(self, report_folder):
        for file in os.listdir('{}/data/{}'.format(os.getcwd(), report_folder)):
            if file.endswith('.csv'):
                with open(f'data/{report_folder}/{file}', 'r', newline='', encoding='utf-8-sig') as raw_csvfile, \
                        open(f'data/clear_{report_folder}/clear_{file}', mode='w', encoding='utf8') as clear_csv:

                    print(f'Start working with file --- {file} ...')
                    reader = csv.reader(raw_csvfile)

                    for _ in range(5):
                        next(reader)

                    raw_headers = next(reader)
                    clear_headers = self.parse_columns_names(raw_headers)

                    writer = self.prepare_header_for_new_csv(clear_csv, clear_headers)

                    for raw_row in reader:
                        row = dict(zip(clear_headers, raw_row))

                        # self.validate_fields(row)

                        writer.writerow(row)

                    print(f'New new file creating ...')


    def prepare_header_for_new_csv(self, file, headers):
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        return writer

    # def validate_fields(self, row):
    #     row['START_DATE_IN_UTC'] = self.transform_date_to_correct_format(row['START_DATE_IN_UTC'])
    #     row['CAMPAIGN_GROUP_START_DATE'] = self.transform_date_to_correct_format(row['CAMPAIGN_GROUP_START_DATE'])
    #
    # def transform_date_to_correct_format(self, date_str):
    #     datetime_obj = datetime.datetime.strptime(date_str, helpers.DATETIME_FORMAT)
    #     return str(datetime_obj.date())


    def run(self, report_folder):
        self.transform_csvs_to_correct_format(report_folder)








if __name__ == '__main__':
    # filename = 'account_507488250_campaign_performance_report_1.1.2017-6.30.2017.csv'
    filename = 'test_clear.csv'

    PerformCsv(config).run(report_folder='campaign_performance')
    # PerformCsv(config).run(report_folder='ad_performance')

