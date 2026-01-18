import variable_keys_beta
import pandas as pd
import sys
import os
import datetime

directory = os.getcwd()
beta_adjustment_file_path = os.path.join(directory, 'inputs', 'inputs.xlsx')
beta_adjustment_df = pd.read_excel(beta_adjustment_file_path, sheet_name='macro_beta_adjustments')

def beta_overrides_by_date(df, method, end_date):
    override_data_df = beta_adjustment_df

    if method =='CDS':
        for index, row in override_data_df.iterrows():
            date_mask = (df['pricedate'] >= datetime.datetime.strftime(row['date_start'], "%Y-%m-%d")) & (df['pricedate'] <= datetime.datetime.strftime(row['date_end'], "%Y-%m-%d"))
            ticker_mask = df.apply(lambda row__original_df: (row__original_df['ticker'] == row['ticker']) if pd.notnull(row['ticker']) else True, axis=1)
            sector_mask = df.apply(lambda row__original_df: (row__original_df['sector'] == row['sector']) if pd.notnull(row['sector']) else True, axis=1)
            region_mask = df.apply(lambda row__original_df: (row__original_df['region'] == row['region']) if pd.notnull(row['region']) else True, axis=1)
            country_mask = df.apply(lambda row__original_df: (row__original_df['country'] == row['country']) if pd.notnull(row['country']) else True, axis=1)
            rating_mask = df.apply(lambda row__original_df: (row__original_df['rating'] == row['rating']) if pd.notnull(row['rating']) else True, axis=1)

            df.loc[date_mask & ticker_mask & sector_mask & region_mask & country_mask & rating_mask, 'beta'] *= row['beta_factor']

    if method =='index':
        # filter out rows where ticker and region are blank -> leaves macro products that aren't impacting all products or irrelevant filters
        override_data_df = override_data_df[override_data_df['ticker'].isna() & (override_data_df['region'].notna()) & override_data_df['country'].isna()]

        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d") if not isinstance(end_date,datetime.datetime) else end_date

        for index, row in override_data_df.iterrows():
            date_mask = (df['pricedate'] >= row['date_start'].strftime('%Y-%m-%d')) & (df['pricedate'] <= row['date_end'].strftime('%Y-%m-%d')) & (df['pricedate'] != end_date.strftime('%Y-%m-%d'))
            region_mask = (df['index_region'] == row['region'])
            combined_mask = date_mask & region_mask

            df = df[~combined_mask]

    return df
