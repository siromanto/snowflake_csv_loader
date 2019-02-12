# -*- coding: utf-8 -*-

import csv
import os
import pandas as pd
import datetime

from configs import config, helpers

INPUT_DATETIME_FORMAT = '%m/%d/%Y'
OUTPUT_DATETIME_FORMAT = "%Y-%m-%d"


class Transform:
    def __init__(self, con_config):
        self.config = con_config

    def parse_columns_names(self, headers):
        return [n.replace('(', '').replace(')', '').replace(" ", "_").replace("-", "_").upper() for n in headers]

    def transform_csvs_to_correct_format(self, report_name):
        helpers.print_header('clear the raw csv file')

        for file in os.listdir('{}/data/{}'.format(os.getcwd(), report_name)):
            if not os.path.exists(f'data/clear_{report_name}'):
                os.makedirs(f'data/clear_{report_name}')

            if file.endswith('.csv'):
                with open(f'data/{report_name}/{file}', 'r', newline='', encoding='utf-8-sig') as raw_csvfile, \
                        open(f'data/clear_{report_name}/clear_{file}', mode='w', encoding='utf8') as clear_csv:

                    print(f'Start working with file --- {file} ...')
                    reader = csv.reader(raw_csvfile)

                    for _ in range(5):  # pass 5 columns with info data
                        next(reader)

                    raw_headers = next(reader)
                    clear_headers = self.parse_columns_names(raw_headers)
                    writer = self.prepare_header_for_clear_csv(clear_csv, clear_headers)

                    for raw_row in reader:
                        row = dict(zip(clear_headers, raw_row))
                        writer.writerow(row)

                    print(f'NEW FILE IS CREATED ...')

    def prepare_header_for_clear_csv(self, file, headers):
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        return writer

    def prepare_csvs_to_load(self, report_name):
        helpers.print_header('transform csv file to load')

        for file in os.listdir(f'{os.getcwd()}/data/clear_{report_name}'):
            if not os.path.exists(f'data/to_load_{report_name}'):
                os.makedirs(f'data/to_load_{report_name}')

            if file.endswith('.csv'):
                print(f'Working with file {file} ===>')

                df = pd.read_csv(f'{os.getcwd()}/data/clear_{report_name}/{file}')
                self.transform_csv_fields(df, case=report_name)
                df.to_csv(f'{os.getcwd()}/data/to_load_{report_name}/to_load_{file}', index=False, encoding='utf-8')

                print(f'DONE WITH FILE ...')

        print('*' * 200)

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

    def run(self, report_name):
        self.transform_csvs_to_correct_format(report_name)
        self.prepare_csvs_to_load(report_name)


if __name__ == '__main__':
    Transform(config).run(report_name='campaign_performance')
    # Transform(config).run(report_name='ad_performance')

