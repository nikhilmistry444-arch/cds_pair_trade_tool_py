
import cds_raw_data
import cds_momentum_signal
import cds_calculator
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
import tranche_delta_runs
import cds_raw_data_cache
import live_data

import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import sys
import pickle
import os

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats

from xbbg import blp

class run_fields: #NOT BEING USED
    def __init__(self,dataframe, bbg_field, column_name):
        self.dataframe = dataframe
        self.bbg_field = bbg_field
        self.column_name = column_name

    def run_formula(self,row):
        return row['bbg_cds_ticker']
    def run_process(self):
        dataframe[self.column_name] = dataframe.apply(run_formula,axis=1)


def cds_index_live_data(dataframe, end_date):

    latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == end_date]

    x = 1
    while latest_index_tranche_quotes_df.empty:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=x)
        latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]
        x += 1

    if datetime.datetime.today().strftime('%Y-%m-%d') in ['2025-03-25']:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=4)
        latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]

    # find latest quote + today's Live Quotes
    if latest_index_tranche_quotes_df[(latest_index_tranche_quotes_df['pricedate'] == end_date) & (latest_index_tranche_quotes_df['macro_product'] == 'index')].empty:

        # finding current live index levels
        today_live_index_quotes_df = latest_index_tranche_quotes_df[latest_index_tranche_quotes_df['macro_product'] == 'index']
        if today_live_index_quotes_df['pricedate'].values[0] != end_date:
            today_live_index_quotes_df.reset_index(drop=True, inplace=True)
            for index, row in today_live_index_quotes_df.iterrows():
                tenor_in_year = variable_keys_beta.tenor_to_year[row['tenor']] if row['tenor'] != '10Y' else '0'
                bbg_cds_ticker = str(variable_keys_beta.cds_index_bbg_core_ticker[row['index_short_name']]) + str(tenor_in_year) + str(row['index_series'])
                ticker = str(bbg_cds_ticker) + str(' CBIN Curncy')
                if row['index_short_name'] == 'CDX HY':
                    field = str('CDS_FLAT_SPREAD')
                else:
                    field = str('PX_MID')
                try:
                    live_quote = blp.bdp(ticker, field).iloc[0, 0]
                    if live_quote == 0 or (pd.isna(live_quote) == True):
                        live_quote, live_quote = row['index_ref_spread_mid'], row['spread_quote']
                        # today_live_all_cds_df.loc[index,'quote_note'] = 'T-1'
                except:  # ISSUE WITH OLDER CDX AND BLOOMBERG IDS. NEED TO FIX
                    live_quote, live_quote = row['index_ref_spread_mid'], row['spread_quote']

                today_live_index_quotes_df.loc[index, 'DoD_ref_spread_chg'] = live_quote - row['spread_quote']
                today_live_index_quotes_df.loc[index, 'pricedate'] = end_date
                today_live_index_quotes_df.loc[index, 'index_ref_spread_mid'] = live_quote
                today_live_index_quotes_df.loc[index, 'spread_quote'] = live_quote


    else:
        today_live_index_quotes_df = latest_index_tranche_quotes_df[(latest_index_tranche_quotes_df['pricedate'] == end_date) & (latest_index_tranche_quotes_df['macro_product'] == 'index')]

    return today_live_index_quotes_df

def cds_tranche_live_data(live_index_dataframe, dataframe, end_date):

    tranche_delta_df = tranche_delta_runs.tranche_deltas()
    latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == end_date]
    x = 1
    while latest_index_tranche_quotes_df.empty:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=x)
        latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]
        x += 1

    if datetime.datetime.today().strftime('%Y-%m-%d') in ['2025-03-25']:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=4)
        latest_index_tranche_quotes_df = dataframe[dataframe['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]

    today_live_index_quotes_df = live_index_dataframe

    # using current live index levels to produce current live tranche quotes
    today_live_index_quotes_df = today_live_index_quotes_df

    if latest_index_tranche_quotes_df[(latest_index_tranche_quotes_df['pricedate'] == end_date).empty & (latest_index_tranche_quotes_df['macro_product'] == 'tranche')].empty:
        today_live_tranche_quotes_df = latest_index_tranche_quotes_df[latest_index_tranche_quotes_df['macro_product'] == 'tranche']
        today_live_tranche_quotes_df.reset_index(drop=True, inplace=True)
        for index, row in today_live_tranche_quotes_df.iterrows():
            try: # assign pre-calculated index spreads. Don't need to re-run again since it uses bbg
                live_ref_quote = today_live_index_quotes_df[(today_live_index_quotes_df['index_short_name']==row['index_short_name']) & (today_live_index_quotes_df['index_series']==row['index_series']) & (today_live_index_quotes_df['tenor']==row['tenor'])]['spread_quote'].values[0]
            except: # missing index quotes
                tenor_in_year = variable_keys_beta.tenor_to_year[row['tenor']] if row['tenor'] != '10Y' else '0'
                bbg_cds_ticker = str(variable_keys_beta.cds_index_bbg_core_ticker[row['index_short_name']]) + str(tenor_in_year) + str(row['index_series'])
                ticker = str(bbg_cds_ticker) + str(' CBIN Curncy')
                if row['index_short_name'] == 'CDX HY':
                    field = str('CDS_FLAT_SPREAD')
                else:
                    field = str('PX_MID')
                try:
                    live_ref_quote = blp.bdp(ticker, field).iloc[0, 0]
                    if live_ref_quote == 0 or (pd.isna(live_ref_quote) == True):
                        live_ref_quote = row['index_ref_spread_mid']
                        # today_live_all_cds_df.loc[index,'quote_note'] = 'T-1'
                except:  # ISSUE WITH OLDER CDX AND BLOOMBERG IDS. NEED TO FIX
                    live_ref_quote = row['index_ref_spread_mid']

            DoD_ref_spread_chg = live_ref_quote - row['index_ref_spread_mid']
            current_spread_quote = row['spread_quote']

            # compute 'spread_quote' for tranches
            if row['macro_product'] == 'tranche' and (row['index_short_name']=='ITRAXX MAIN' or row['index_short_name']=='ITRAXX XOVER' ): #only set it up for itrx main and xover!!!!
                # CAN IMPROVE. USING 5YEAR DELTA
                tenor_in_year = variable_keys_beta.tenor_to_year[row['tenor']]
                dictionary_finder = str(row['index_short_name'].replace(' ','_')) +str('_') + str(5)
                bbg_cds_ticker = str(variable_keys_beta.cds_index_bbg_core_ticker[row['index_short_name']]) + str(5) + str(row['index_series'])
                attachment_detachment = str(int(row['attachment']*100)) + str('-') + str(int(row['detachment']*100))
                try:
                    tranche_delta = tranche_delta_df[(tranche_delta_df['index_short_name_generic']==dictionary_finder) & (tranche_delta_df['bbg_index_number']==bbg_cds_ticker) & (tranche_delta_df['attachment-detachment']==attachment_detachment)]['delta'].values[0]
                except Exception as e:
                    tranche_delta = tranche_delta_runs.backup_tranche_deltas(df=tranche_delta_df, dictionary_finder=dictionary_finder, attachment_detachment=attachment_detachment, current_series=row['index_series'])

                today_live_tranche_quotes_df.loc[index, 'spread_quote'] = current_spread_quote + (DoD_ref_spread_chg * tranche_delta)
            elif row['macro_product'] == 'index':
                today_live_tranche_quotes_df.loc[index, 'spread_quote'] = live_ref_quote
            else: # NEED TO IMPROVE
                today_live_tranche_quotes_df.loc[index, 'spread_quote'] = row['spread_quote'] + DoD_ref_spread_chg

            today_live_tranche_quotes_df.loc[index, 'DoD_ref_spread_chg'] = DoD_ref_spread_chg
            today_live_tranche_quotes_df.loc[index, 'index_ref_spread_mid'] = live_ref_quote
            today_live_tranche_quotes_df.loc[index, 'pricedate'] = end_date
    else:
        today_live_tranche_quotes_df = latest_index_tranche_quotes_df[(latest_index_tranche_quotes_df['pricedate'] == end_date) & (latest_index_tranche_quotes_df['macro_product'] == 'tranche')]

    return today_live_tranche_quotes_df

def cds_live_data(df, date_range_end):

    # find current quote(T-1) + Todays Live Quotes
    current_all_cds_df = df[df['pricedate'] == date_range_end]

    x = 1
    latest_all_cds_df = current_all_cds_df
    while latest_all_cds_df.empty:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=x)
        latest_all_cds_df = df[df['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]
        x += 1

    # assign today's live quotes - bbg source - mid levels like historic data
    today_live_all_cds_df = latest_all_cds_df
    if today_live_all_cds_df['pricedate'].values[0] != date_range_end:
        today_live_all_cds_df.reset_index(drop=True, inplace=True)
        for index, row in today_live_all_cds_df.iterrows():
            bbg_cds_ticker = row['bbg_cds_ticker']
            ticker = str(bbg_cds_ticker) + str(' MSG1 Curncy')
            tenor = row['tenor']
            field = str(tenor) + str('_MID_CDS_SPREAD')
            try:
                live_quote = blp.bdp(ticker, field).iloc[0, 0]
                if live_quote == 0 or (pd.isna(live_quote) == True):
                    live_quote = row['quote']
                    # today_live_all_cds_df.loc[index,'quote_note'] = 'T-1'
            except:
                live_quote = row['quote']
            today_live_all_cds_df.loc[index, 'pricedate'] = date_range_end
            today_live_all_cds_df.loc[index, 'quote'] = live_quote

    current_all_cds_df = today_live_all_cds_df

    return current_all_cds_df

def fx_live_data(df, end_date):

    current_fx_df = df[df['pricedate'] == end_date]
    latest_fx_df = current_fx_df

    # toggle to find latest snap saved in the database
    x = 1
    while latest_fx_df.empty:
        latest_date_check = datetime.datetime.today() - datetime.timedelta(days=x)
        latest_fx_df = df[df['pricedate'] == latest_date_check.strftime('%Y-%m-%d')]
        x += 1

    todays_live_fx_df = latest_fx_df
    if todays_live_fx_df['pricedate'].values[0] != end_date:
        todays_live_fx_df.reset_index(drop=True, inplace=True)
        for index, row in todays_live_fx_df.iterrows():
            try:
                live_quote = blp.bdp(row['bloomberg_ticker'], field).iloc[0, 0]
                if live_quote == 0 or (pd.isna(live_quote) == True):
                    live_quote = row['quote']
                    # today_live_all_cds_df.loc[index,'quote_note'] = 'T-1'
            except:
                live_quote = row['quote']
            todays_live_fx_df.loc[index, 'pricedate'] = end_date
            todays_live_fx_df.loc[index, 'quote'] = live_quote

    return todays_live_fx_df