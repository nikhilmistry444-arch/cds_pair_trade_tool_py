import cds_raw_data
import variable_keys_beta
import variable_keys_beta
import spread_ranges_generator
import cds_momentum_signal

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

def save_data(data, directory ,filename):
    if not os.path.exists(directory):
        os.makedirs(directory)
    filepath = os.path.join(directory, filename)
    with open(filepath, 'wb') as f:
        # Save data along with the current date
        pickle.dump({'date': datetime.datetime.now().date(), 'data': data}, f)

def load_saved_data(directory, filename):
    filepath = os.path.join(directory, filename)
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            saved_data = pickle.load(f)
            # Check if the saved date matches today's date
            if saved_data['date'] == datetime.datetime.now().date():
                return saved_data['data']
    return None

def load_data(module, function, directory, filename, start_date=None, end_date=None, pricing_source=None, data_frame=None, process=None, actual_end_date=None, maturity_range=None):

    # load any data that's cached
    output_dataframe = load_saved_data(directory=directory, filename=filename)

    if output_dataframe is None:

        if (process == 'calculate_cds_momentums'):
            data_frame['momentum'], data_frame['momentum_spread'] = zip(*data_frame.apply(lambda row: cds_momentum_signal.ranking_tree(
                        spread_dataframe_ranges=spread_ranges_generator.find_spread_ranges(cds_spread_range_df=pricing_source,sector=[row['sector']], seniority=[row['seniority']], region=[row['region']], tenor=[row['tenor']]),
                        sector=row['sector'], region=row['region'], country_two_digit=row['country'], ticker=row['ticker'],rating=row['rating'], seniority=row['seniority'], tenor=row['tenor']), axis=1))
            output_dataframe = data_frame

        elif (process == 'beta_adjust_historical_attribute_changes_vs_now'):
            beta_adjust_historical_attribute_changes_vs_now_instance = variable_keys_beta.beta_adjust_historical_attribute_changes_vs_now(start_date=start_date, end_date=end_date, df=data_frame)
            output_dataframe = getattr(beta_adjust_historical_attribute_changes_vs_now_instance, function)()
        elif (process == 'fx_rates'):
            output_dataframe = getattr(module, function)(start_date, actual_end_date)
        elif (start_date is None) and (end_date is None) and (pricing_source is None) and (data_frame is None) and (process is None) and (actual_end_date is None):
            output_dataframe = getattr(module, function)()

        elif (data_frame is None) and (process is None):
            output_dataframe = getattr(module, function)(start_date, end_date, pricing_source)

        else:
            output_dataframe = getattr(module, function)(start_date, end_date, data_frame)
        save_data(output_dataframe, directory=directory, filename=filename)
    else:
        print(f'{filename} loaded from cache')


    if filename == 'index_tranche_quotes_df.pickle':
        #data_end_date = datetime.datetime.strptime(actual_end_date, "%Y-%m-%d") if not isinstance(actual_end_date, datetime.datetime) else actual_end_date
        output_dataframe = output_dataframe[output_dataframe['pricedate'] <=actual_end_date]
        # output_dataframe['index_maturity_years'] = (((output_dataframe['index_maturity'] - data_end_date).dt.days) / 365.25)
        output_dataframe['index_maturity_years'] = (pd.to_datetime(output_dataframe['index_maturity']) - pd.to_datetime(output_dataframe['pricedate'])).dt.days / 365.25


    return output_dataframe
