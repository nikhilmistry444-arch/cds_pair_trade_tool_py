import pandas as pd
import numpy as np
import variable_keys_beta
from scipy import stats

rating_key = {
        'AAA': 'AAA',
        'AA+': 'AA+',
        'AA': 'AA',
        'AA-': 'AA-',
        'A+': 'A+',
        'A': 'A',
        'A-': 'A-',
        'BBB+': 'BBB+',
        'BBB': 'BBB',
        'BBB-': 'BBB-',
        'BB+': 'BB+',
        'BB': 'BB',
        'BB-': 'BB-',
        'B+': 'B+',
        'B': 'B',
        'B-': 'B-',
        'CCC+': 'CCC+',
        'CCC': 'CCC',
        'CCC-': 'CCC-',
        'CC': 'CC',
        'C': 'C',
        'D': 'D',
        'DDD': 'DDD',
        'WR': 'WR',
        'NR': 'NR',
        'Aaa': 'AAA',
        'Aa1': 'AA+',
        'Aa2': 'AA',
        'Aa3': 'AA-',
        'A1': 'A+',
        'A2': 'A',
        'A3': 'A-',
        'Baa1': 'BBB+',
        'Baa2': 'BBB',
        'Baa3': 'BBB-',
        'Ba1': 'BB+',
        'Ba2': 'BB',
        'Ba3': 'BB-',
        'B1': 'B+',
        'B2': 'B',
        'B3': 'B-',
        'Caa1': 'CCC+',
        'Caa2': 'CCC',
        'Caa3': 'CCC-',
        'Ca': 'CC',
        'NULL': 'NR',
        None: 'NR',
        'nan': 'NR',
        'NA': 'NR'
    }

tenor_multiplier = {
    '6M': 0.1,
    '1Y': 0.3,
    '2Y': 0.5,
    '3Y': 0.6,
    '4Y':0.8,
    '5Y': 1,
    '7Y': 1.5,
    '10Y': 2,
    '15Y': 2.75,
    '20Y': 3.5,
    '30Y': 4
}

def credit_duration_multiplier(row):
    if float(row['credit_duration']) <=2:
        return 0.7
    elif 2 < float(row['credit_duration']) <= 3:
        return 0.8
    elif 3 < float(row['credit_duration']) <= 5:
        return 0.9
    elif 5 < float(row['credit_duration']) <= 7:
        return 1
    elif 7 < float(row['credit_duration']) <= 10:
        return 1.4
    elif 10 < float(row['credit_duration']) <= 13:
        return 1.6
    elif 13 < float(row['credit_duration']) <= 15:
        return 1.8
    elif 15 < float(row['credit_duration']) <= 20:
        return 2
    elif 20 < float(row['credit_duration']) <= 30:
        return 2.25
    elif 30 < float(row['credit_duration']) <= 50:
        return 2.5
    elif 50 < float(row['credit_duration']) <= 100:
        return 3
    elif 100 < float(row['credit_duration']):
        return 3.5


region_multiplier = {
    'AMERICAS':1.1,
    'EMEA':0.9,
    'ASIA':0.9,
    'EM':1.4
}

ccy_multiplier = {
    'EUR':0.9,
    'GBP':1,
    'USD':1.2
}

seniority_multiplier = {
    '1st Lien':0.6,
    '1.5 Lien':0.7,
    '2nd Lien':0.8,
    '3rd Lien':0.85,
    'Asset Backed':0.9,
    'Sr Secured':0.9,
    'Secured':0.9,
    'Sr Preferred':1,
    'Sr Unsecured':1,
    'Sr Unsec':1,
    'Sr Non Preferred':1.2,
    'Unsecured':1.2,
    'Sr Subordinated':3,
    'Subordinated':3.5,
    'Sub':3.5,
    'Jr Subordinated':4,
    'Jr Sub':5
}

rating_multiplier = {
    'AAA':0.65,
    'AA+':0.7,
    'AA':0.75,
    'AA-':0.8,
    'A+':0.85,
    'A':0.9,
    'A-':0.95,
    'BBB+':1,
    'BBB':1.2,
    'BBB-':1.3,
    'BB+':2.7,
    'BB':4,
    'BB-':4.5,
    'B+':5,
    'B':6.5,
    'B-':7,
    'CCC+':8,
    'CCC':8.5,
    'CCC-':12,
    'CC':15,
    'C':18,
    'D':22,
    'DDD':25,
    'WR':1.2,
    'NR':1.2,
    'Aaa':0.65,
    'Aa1':0.7,
    'Aa2':0.75,
    'Aa3':0.8,
    'A1':0.85,
    'A2':0.9,
    'A3':0.95,
    'Baa1':1,
    'Baa2':1.2,
    'Baa3':1.3,
    'Ba1':2.7,
    'Ba2':4,
    'Ba3':4.5,
    'B1':5,
    'B2':6.5,
    'B3':7,
    'Caa1':8,
    'Caa2':8.5,
    'Caa3':12,
    'Ca':18,
    'NULL':1.2,
    'nan':1.2
}

sector_multiplier = {
    'Communication Services': 1,
    'Consumer Discretionary':1.3,
    'Consumer Staples':1,
    'Energy':0.9,
    'Financials':1,
    'Government':0.8,
    'Health Care':1,
    'Industrials':1,
    'Information Technology':1,
    'Materials':1.2,
    'Real Estate':1.2,
    'Utilities':0.9
}

def multiplier_lists(set):
    name = set+'_multiplier'
    if name=='ccy_multiplier':
        return ccy_multiplier
    elif name=='region_multiplier':
        return region_multiplier
    elif name=='sector_multiplier':
        return sector_multiplier
    elif name=='rating_multiplier':
        return rating_multiplier
    elif name=='credit_duration_multiplier':
        return credit_duration_multiplier
    elif name=='payment_rank_multiplier':
        return payment_rank_multiplier



rating_rank = {
    'AAA':1,
    'AA+':2,
    'AA':3,
    'AA-':4,
    'A+':5,
    'A':6,
    'A-':7,
    'BBB+':8,
    'BBB':9,
    'BBB-':10,
    'BB+':11,
    'BB':12,
    'BB-':13,
    'B+':14,
    'B':15,
    'B-':16,
    'CCC+':17,
    'CCC':18,
    'CCC-':19,
    'CC':20,
    'C':21,
    'D':22,
    'DDD':23,
}

tenor_to_year = {
    '1W': 1/52,
    '2W': 2/52,
    '3W': 3/52,
    '1M': 1/12,
    '2M': 2/12,
    '3M': 3/12,
    '4M': 4/12,
    '6M': 0.5,
    '1Y': 1,
    '2Y': 2,
    '3Y': 3,
    '4Y': 4,
    '5Y': 5,
    '7Y': 7,
    '10Y': 10,
    '15Y': 15,
    '20Y': 20,
    '30Y': 30
}

year_to_tenor = {
    0.5: '6M',
    1: '1Y',
    2: '2Y',
    3: '3Y',
    4: '4Y',
    5: '5Y',
    7: '7Y',
    10: '10Y',
    15: '15Y',
    20: '20Y',
    30: '30Y'
}

cds_index_recovery_rate = {
    'CDX IG':0.4,
    'CDX HY':0.4,
    'CDX EM':0.4,
    'ITRAXX MAIN':0.4,
    'ITRAXX XOVER':0.4,
    'ITRAXX FINS SEN':0.4,
    'ITRAXX FINS SUB':0.4
}

cds_index_currency_key = {
    'CDX IG':'USD',
    'CDX HY':'USD',
    'CDX EM':'USD',
    'ITRAXX MAIN':'EUR',
    'ITRAXX XOVER':'EUR',
    'ITRAXX FINS SNR':'EUR',
    'ITRAXX FINS SUB':'EUR'
}

cds_index_region_exposure = {
    'CDX IG': 'AMERICAS',
    'CDX HY': 'AMERICAS',
    'CDX EM': 'EM',
    'ITRAXX MAIN': 'EMEA',
    'ITRAXX XOVER': 'EMEA',
    'ITRAXX FINS SNR': 'EMEA',
    'ITRAXX FINS SUB': 'EMEA'
}

cds_index_ig_hy = {
    'CDX IG':'IG',
    'CDX HY':'HY',
    'CDX EM':'HY',
    'ITRAXX MAIN':'IG',
    'ITRAXX XOVER':'HY',
    'ITRAXX FINS SNR':'IG',
    'ITRAXX FINS SUB':'HY'
}

cds_index_bbg_core_ticker = {
    'CDX IG':'CDXIG',
    'CDX HY':'CXPHY',
    'CDX EM':'CDXEM',
    'ITRAXX MAIN':'ITXEB',
    'ITRAXX XOVER':'ITXEX',
    'ITRAXX FINS SNR':' ITXES',
    'ITRAXX FINS SUB':'ITXEU'
}

# sub labels



cdx_em_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.15, 0.25, 0.35, 0],
    'detachment': [0.15, 0.25, 0.35, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
cdx_ig_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.03, 0.07, 0.15, 0],
    'detachment': [0.03, 0.07, 0.15, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
cdx_hy_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.15, 0.25, 0.35, 0],
    'detachment': [0.15, 0.25, 0.35, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
itraxx_main_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.03, 0.06, 0.12, 0],
    'detachment': [0.03, 0.06, 0.12, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
itraxx_fins_snr_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.03, 0.06, 0.12, 0],
    'detachment': [0.03, 0.06, 0.12, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
itraxx_xover_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.1, 0.2, 0.35, 0],
    'detachment': [0.1, 0.2, 0.35, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})
itraxx_fins_sub_index_tranche_sub_label = pd.DataFrame({
    'attachment': [0, 0.1, 0.2, 0.35, 0],
    'detachment': [0.1, 0.2, 0.35, 1, 1],
    'sub_level': ['equity','junior mezzanine','senior mezzanine','super senior', 'index']})

def find_index_tranche_sub_level(index_short_name, attachment, detachment):
    att_detach = str(int(attachment) if attachment == 1 or attachment == 0 else attachment) + '-' + str(int(detachment) if detachment == 1 or detachment == 0 else detachment)
    if index_short_name == 'CDX HY':
        cost_bp = cdx_hy_index_tranche_bp_cost[att_detach]
    elif index_short_name == 'CDX IG':
        cost_bp = cdx_ig_index_tranche_bp_cost[att_detach]
    elif index_short_name == 'CDX EM':
        cost_bp = cdx_em_index_tranche_bp_cost[att_detach]
    elif index_short_name in ['ITRAXX XOVER', 'ITRAXX FINS SUB']:
        cost_bp = itraxx_hy_index_tranche_bp_cost[att_detach]
    elif index_short_name in ['ITRAXX MAIN', 'ITRAXX FINS SNR']:
        cost_bp = itraxx_ig_index_tranche_bp_cost[att_detach]
    else:
        cost_bp = 3


class beta_adjust_historical_attribute_changes_vs_now:
    def __init__(self, start_date, end_date, df):
        # apply adjustments to historic beta vs current/todays beta attribute by attribute
        self.df = df
        self.start_date = start_date
        self.end_date = end_date

        self.current_cds_df = df[df['pricedate'] == df['pricedate'].max()].reset_index()
        self.unique_cds_df = self.current_cds_df.drop_duplicates(subset=['bbg_cds_ticker', 'tenor', 'currency'])
        self.attribute_list = ['rating']

    def adjust_rating(self, row):
        current_rating = self.current_all_cds_df_unique_cds.loc[
            (self.current_all_cds_df_unique_cds['bbg_cds_ticker'] == row['bbg_cds_ticker']) &
            (self.current_all_cds_df_unique_cds['tenor'] == row['tenor']) &
            (self.current_all_cds_df_unique_cds['currency'] == row['currency']), 'rating'].values[0]

        rating_lag_adjuster_days = 60
        rating_lag_date = (pd.to_datetime(row['pricedate']) + pd.Timedelta(days=rating_lag_adjuster_days)).strftime('%Y-%m-%d')
        row_rating = row['rating']

        while rating_lag_date <= self.current_all_cds_df_unique_cds['pricedate'].values[0]:
            try:
                if (self.subset_all_cds_df_unique_cds[self.subset_all_cds_df_unique_cds['pricedate'] == rating_lag_date]['rating'].isnull().any()) or \
                        (self.subset_all_cds_df_unique_cds[self.subset_all_cds_df_unique_cds['pricedate'] == rating_lag_date]['rating'].empty) or \
                        (self.subset_all_cds_df_unique_cds[self.subset_all_cds_df_unique_cds['pricedate'] == rating_lag_date]['rating'].values == '').any():
                    rating_lag_date = (pd.to_datetime(rating_lag_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    row_rating = self.subset_all_cds_df_unique_cds.loc[self.subset_all_cds_df_unique_cds['pricedate'] == rating_lag_date, 'rating'].values[0]
                    break
            except:
                rating_lag_date = (pd.to_datetime(rating_lag_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                print((self.subset_all_cds_df_unique_cds[self.subset_all_cds_df_unique_cds['pricedate'] == rating_lag_date]['rating']))

        beta_multiplier_current_rating = variable_keys_beta.rating_multiplier[current_rating]
        beta_multiplier_row_rating = variable_keys_beta.rating_multiplier[row_rating]
        rating_beta = beta_multiplier_row_rating / beta_multiplier_current_rating

        return row['beta'] * rating_beta

    def run_processes(self):

        self.new_df = pd.DataFrame()

        for unique_index, unique_row in self.unique_cds_df.iterrows():  # unique list of cds to run for
            # Create a subset of the DataFrame based on conditions
            self.subset_all_cds_df_unique_cds = self.df[
                (self.df['bbg_cds_ticker'] == unique_row['bbg_cds_ticker']) &
                (self.df['tenor'] == unique_row['tenor']) &
                (self.df['currency'] == unique_row['currency']) &
                (self.df['pricedate'] >= self.start_date) &
                (self.df['pricedate'] <= self.end_date)
                ]

            # Get the latest pricedate for each unique combination
            latest_pricedate = self.subset_all_cds_df_unique_cds.groupby(['bbg_cds_ticker', 'tenor', 'currency'])['pricedate'].transform('max')
            self.current_all_cds_df_unique_cds = self.subset_all_cds_df_unique_cds[self.subset_all_cds_df_unique_cds['pricedate'] == latest_pricedate]

            # apply adjustment by rating
            self.subset_all_cds_df_unique_cds['beta'] = self.subset_all_cds_df_unique_cds.apply(lambda row: beta_adjust_historical_attribute_changes_vs_now.adjust_rating(self, row), axis=1)
            self.new_df = pd.concat([self.new_df, self.subset_all_cds_df_unique_cds], ignore_index=True)

        return self.new_df

class calculate_beta:

    @staticmethod
    def for_index_tranches(data_set, method='regression'):

        if method == 'regression':
            # regression method (note: rvalue is correlation coefficient)
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                data_set['spread_quote_x'], data_set['spread_quote_y'])
            beta_ratio_pair = slope

        elif method == 'dod_change_vs_change':
            for i in range(len(data_set)):
                if i == 0:
                    continue
                else:
                    data_set.loc[i,'ratio_dod'] = abs(data_set.loc[i, 'spread_quote_y'] - data_set.loc[i-1, 'spread_quote_y'])/ abs(data_set.loc[i, 'spread_quote_x'] - data_set.loc[i-1, 'spread_quote_x'])
                    #dod_diff_data_sets.loc[i, 'ratio_dod'] = (dod_diff_data_sets.loc[i, 'spread_quote_y'] - dod_diff_data_sets.loc[i - 1, 'spread_quote_y']) / (dod_diff_data_sets.loc[i, 'spread_quote_x'] - dod_diff_data_sets.loc[i - 1, 'spread_quote_x'])
            beta_ratio_pair = (data_set['ratio_dod'].mean())

        else:
            beta_ratio_pair = 1

        return beta_ratio_pair