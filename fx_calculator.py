
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



def fx_tag_finder(ccy, fx_conversion, fx_rates_df, date):

    if (ccy == fx_conversion) or ccy=='local':
        return str(fx_conversion)+str(fx_conversion), 1

    # Try direct pair: e.g., EURUSD
    fx_tag_direct = f"{ccy}{fx_conversion}"
    fx_row = fx_rates_df[(fx_rates_df['pricedate'] == date) & (fx_rates_df['fx_tag'] == fx_tag_direct)]

    if not fx_row.empty:
        return fx_tag_direct , 'not inverted'

    # Try inverse pair: e.g., USDEUR → invert
    fx_tag_inverse = f"{fx_conversion}{ccy}"
    fx_row = fx_rates_df[(fx_rates_df['pricedate'] == date) & (fx_rates_df['fx_tag'] == fx_tag_inverse)]

    if not fx_row.empty:
        return fx_tag_inverse , 'inverted'

def fx_rate_on_date(ccy, fx_conversion, fx_rates_df, date):
    """
    Returns the FX rate to convert from `ccy` to `fx_conversion` on a given date.

    Parameters:
        ccy (str): Source currency (e.g., 'EUR')
        fx_conversion (str): Target currency (e.g., 'USD')
        fx_rates_df (pd.DataFrame): DataFrame with FX rates
        date (datetime or str): Date to look up the FX rate

    Returns:
        float: FX rate (e.g., EUR to USD rate)
    """
    if (ccy == fx_conversion) or ccy=='local':
        return 1.0

    # Try direct pair: e.g., EURUSD
    fx_tag_direct = f"{ccy}{fx_conversion}"
    fx_row = fx_rates_df[(fx_rates_df['pricedate'] == date) & (fx_rates_df['fx_tag'] == fx_tag_direct)]

    if not fx_row.empty:
        return fx_row.iloc[0]['quote']

    # Try inverse pair: e.g., USDEUR → invert
    fx_tag_inverse = f"{fx_conversion}{ccy}"
    fx_row = fx_rates_df[(fx_rates_df['pricedate'] == date) & (fx_rates_df['fx_tag'] == fx_tag_inverse)]

    if not fx_row.empty:
        return 1 / fx_row.iloc[0]['quote']

    # If neither found, raise error
    raise ValueError(f"FX rate for {ccy}/{fx_conversion} on {date} not found.")

def fx_column_trades_analysis(trade_date, df, fx_ccy, fx_conversion,fx_rates_df):

    # coupon payment schedule
    coupon_frequency = 4
    coupon_day = 20
    coupon_months = [3,6,12]

    fx_tag, fx_direction = fx_tag_finder(ccy=fx_ccy, fx_conversion=fx_conversion, date=trade_date, fx_rates_df=fx_rates_df)
    df['fx_tag'] = fx_tag

    #find the daily fx rate
    fx_rates_df = fx_rates_df.drop(columns='bloomberg_ticker')
    df = pd.merge(df, fx_rates_df, how='left', left_on=['pricedate','fx_tag'], right_on=['pricedate','fx_tag'])
    df.rename(columns={'quote':'fx_rate_daily'}, inplace=True)
    df['fx_rate_daily'] = df['fx_rate_daily'].ffill().bfill()
    if fx_direction == 1:
        df['fx_rate_daily'] = 1
    elif fx_direction == 'inverted':
        df['fx_rate_daily'] = 1/df['fx_rate_daily']
    else:
        pass

    # dont need to do. mark to market moves from fx is needed. rather than the payment schedule every 3months. when fx realised. accrual will account for it
    #df['fx_rate_accrual']

    return df





