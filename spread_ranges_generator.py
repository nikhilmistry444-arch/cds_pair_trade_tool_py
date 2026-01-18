import cds_raw_data
import variable_keys_beta

import pandas as pd
from pandas.tseries.offsets import BDay
from scipy.optimize import curve_fit
import numpy as np
import sys
import pickle
import os

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats


def find_spread_ranges(cds_spread_range_df, sector, seniority, region, tenor):
    range_df = cds_spread_range_df.loc[(cds_spread_range_df['seniority'].isin(seniority)) & (cds_spread_range_df['tenor'].isin(tenor)) & (cds_spread_range_df['sector'].isin(sector)) & (cds_spread_range_df['region'].isin(region))] #filter for data we need to speed code
    range_df['rating'] = range_df['rating'].map(variable_keys_beta.rating_key) #clean rating to S&P format
    range_df['rating_rank'] = range_df['rating'].map(variable_keys_beta.rating_rank) #produce rating rank to order
    range_df = range_df.dropna(subset=['rating']) #remove nan ratings ####can improve raw data so all nan ratings are rated

    # find unique sets
    spread_range_df = pd.DataFrame()
    unique_sets = range_df[['sector', 'seniority', 'region', 'tenor']].drop_duplicates()
    for index, row in unique_sets.iterrows():
        unique_sector = row['sector']
        unique_seniority = row['seniority']
        unique_region = row['region']
        unique_tenor = row['tenor']
        unique_spread_range = range_df.loc[(range_df['sector'] == unique_sector) & (range_df['seniority'] == unique_seniority) & (range_df['region'] == unique_region) & (range_df['tenor'] == unique_tenor)]

        # create dataframe base
        new_unique_df = pd.DataFrame(variable_keys_beta.rating_rank.items(), columns=['rating', 'rating_rank'])
        new_unique_df['sector'] = unique_sector
        new_unique_df['seniority'] = unique_seniority
        new_unique_df['region'] = unique_region
        new_unique_df['tenor'] = unique_tenor
        new_unique_df = new_unique_df.sort_values(by=['rating_rank'], ascending=True)

        # left join current spread ranges
        new_unique_df = pd.merge(new_unique_df, unique_spread_range, how='left', on=['rating', 'sector', 'seniority', 'region', 'tenor'])
        new_unique_df = new_unique_df.drop('rating_rank_y', axis=1)
        new_unique_df = new_unique_df.rename(columns={'rating_rank_x': 'rating_rank'})

        # ensure curve is upward sloping
        for i in range(len(new_unique_df)):
            if i == 0:
                new_unique_df.loc[i, 'quote'] = 0 if pd.isna(new_unique_df.loc[i, 'quote']) else new_unique_df.loc[i, 'quote']
                continue
            elif pd.isna(new_unique_df.loc[i, 'quote']) == False: #if quote exists
                for u in range(1, i+1):
                    u = i - u
                    if pd.isna(new_unique_df.loc[u, 'quote'])==  False: #check quote with quote below(d) and above(u)
                        for d in range(1, len(new_unique_df) - i):  # check next available quote is larger than current
                            d = i + d
                            if pd.isna(new_unique_df.loc[d, 'quote'])==  False: #make sure d quote>u quote
                                if (new_unique_df.loc[d, 'quote'] <= new_unique_df.loc[i, 'quote']) \
                                        and (new_unique_df.loc[u, 'quote'] <= new_unique_df.loc[i, 'quote']) \
                                        and (new_unique_df.loc[d, 'quote'] >= new_unique_df.loc[u, 'quote']): #compare above and below to treat
                                    rank_diff_range = new_unique_df.loc[d, 'rating_rank'] - new_unique_df.loc[u, 'rating_rank']
                                    quote_diff = new_unique_df.loc[d, 'quote'] - new_unique_df.loc[u, 'quote']
                                    c_intercept = new_unique_df.loc[u, 'quote']
                                    m_slope = quote_diff / rank_diff_range
                                    new_unique_df.loc[i, 'quote'] = c_intercept + (m_slope * ((new_unique_df.loc[i, 'rating_rank']-new_unique_df.loc[u, 'rating_rank'])))
                                    break
                                else:
                                    continue
                            else:
                                continue
                        break #continue to next if quote is found
                    else:
                        continue
            elif i == (len(new_unique_df)-1):  # ####CAN BE IMPROVED!!!!!!!!!!!
                new_unique_df.loc[i, 'quote'] = 3000 if pd.isna(new_unique_df.loc[i, 'quote']) else new_unique_df.loc[i, 'quote']
                continue
            else:
                continue

        #curve fit based on table ###LINEAR INTERPOLATION BETWEEN DATA POINTS
        # ####CAN BE IMPROVED!!!!!!!!!!!
        for i in range(len(new_unique_df)):
            if pd.isna(new_unique_df.loc[i, 'quote']) == True:
                for u in range(1, i + 1):
                    u = i - u
                    if pd.isna(new_unique_df.loc[u, 'quote'])==  False:
                        break
                    else:
                        continue

                for d in range(1, len(new_unique_df) - i):
                    d = i + d
                    if pd.isna(new_unique_df.loc[d, 'quote'])==  False:
                        break
                    else:
                        continue

                rank_diff_range = new_unique_df.loc[d, 'rating_rank'] - new_unique_df.loc[u, 'rating_rank']
                quote_diff = new_unique_df.loc[d, 'quote'] - new_unique_df.loc[u, 'quote']
                c_intercept = new_unique_df.loc[u, 'quote']
                m_slope = quote_diff / rank_diff_range
                new_unique_df.loc[i, 'quote'] = c_intercept + (m_slope * ((new_unique_df.loc[i, 'rating_rank'] - new_unique_df.loc[u, 'rating_rank'])))
            else:
                continue

        ###NOT WORKING. RETURNS ONE VALUE FOR ALL X_INPUTS WHHY CAUSE IT CANT FIND A CURVE GOOD ENOUGH
        # new_unique_df_core = new_unique_df.dropna(subset=['quote'])
        # xdata = np.array(new_unique_df_core['rating_rank'])
        # ydata = np.array(new_unique_df_core['quote'])
        #
        # # add missing spread quotes to ratings using curve fit
        # for i in range(len(new_unique_df)):
        #     if pd.isna(new_unique_df.loc[i, 'quote']) == True:
        #         x_input = new_unique_df.loc[i, 'rating_rank']
        #         new_unique_df.loc[i, 'quote'] = curve_creator.fitted_func(x_input=x_input ,xdata=xdata, ydata=ydata)


        # add to main dataframe
        spread_range_df = new_unique_df.append(spread_range_df, ignore_index=True)

    # 1 upward slop spread ranges  DONE
    # 2 add ratings and ranks for areas without data
    # 3 add quotes based on curve creator below

    return spread_range_df





def historic_spread_ranges_generator(data_to_run, data_quotes, start_range, end_range):
    maturity_years_at_the_time_data_quotes = data_quotes
    maturity_years_at_the_time_data_quotes['index_maturity_years_then'] = (pd.to_datetime(maturity_years_at_the_time_data_quotes['index_maturity']) - pd.to_datetime(maturity_years_at_the_time_data_quotes['pricedate'])).dt.days/365.25

    # find exact products historical data
    exact_filtered_data_quotes = maturity_years_at_the_time_data_quotes[(maturity_years_at_the_time_data_quotes['index_short_name'] == data_to_run.index_short_name) &
                                                                   #(maturity_years_at_the_time_data_quotes['tenor'] == data_to_run.tenor) &
                                                                   (maturity_years_at_the_time_data_quotes['attachment'] == data_to_run.attachment) &
                                                                   (maturity_years_at_the_time_data_quotes['detachment'] == data_to_run.detachment) &
                                                                   (maturity_years_at_the_time_data_quotes['index_maturity_years_then'] <= (data_to_run.index_maturity_years+0.25)) & # looking back for the last 3 months
                                                                   (maturity_years_at_the_time_data_quotes['index_maturity_years_then'] >= (data_to_run.index_maturity_years-0.25)) &
                                                                   (maturity_years_at_the_time_data_quotes['pricedate'] >= start_range) &
                                                                   (maturity_years_at_the_time_data_quotes['pricedate'] <= end_range)]
    # clean the data. spread quotes should not be negative
    exact_filtered_data_quotes = exact_filtered_data_quotes[exact_filtered_data_quotes['spread_quote'] >= 0]



    # find the spread ranges
    if exact_filtered_data_quotes.empty:
        results_string = ''
    else:
        max_spread = int(exact_filtered_data_quotes['spread_quote'].max())
        min_spread = int(exact_filtered_data_quotes['spread_quote'].min())
        average_spread = int(exact_filtered_data_quotes['spread_quote'].mean())
        percentile_spread_quote = int(stats.percentileofscore(exact_filtered_data_quotes['spread_quote'], data_to_run.spread_quote))
        results_string = str('[') + str(min_spread) + str(' - ') + str(max_spread) + str(']') + str(' ') + str(percentile_spread_quote) + str(' %ile')

    return results_string



class curve_creator(): ###NOT WORKING. RETURNS ONE VALUE FOR ALL X_INPUTS
    def __init__(self, xdata, ydata, x_input):
        self.xdata = xdata
        self.ydata = ydata
        self.x_input = x_input
    def func(x, a, b, c):
        return a * np.exp(-b * x) + c
    def fitted_func(x_input, xdata, ydata):
        popt, pcov = curve_fit(curve_creator.func, xdata, ydata, maxfev=100)
        return popt[0] * np.exp(-popt[1] * x_input) + popt[2]



