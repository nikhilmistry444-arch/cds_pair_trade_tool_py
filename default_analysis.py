import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
import re


def weibull_curve(x,a,b,c):
    return a * (1 - np.exp(- (x / b) ** c))

def risk_extrapolation_to_duration(duration):

    if duration == 0:
        base_risk_multiplier=0
    else:
        base_risk_year = 5 # assume all risk based off 5 years. then adjust expect loss from this
        base_risk_year = duration/5

        ########fit the weibull CDF##############
        # improvement. update weibull cdf function to fit to real credit data
        # Target constraints
        x_vals = np.array([5, 10])
        y_vals = np.array([1.0, 1.3])
        # Fit the parameters
        params, _ = curve_fit(weibull_curve, x_vals, y_vals, bounds=(0, [10, 20, 5]))
        # Unpack fitted parameters
        a, b, c = params
        # Evaluate the function at x = 2
        x_query = duration
        y_query = weibull_curve(x_query, a, b, c)
        base_risk_multiplier = y_query

    return base_risk_multiplier

# can improve the below (5yr points)
    # doesnt include CDX EM and others
    # make it more dynamic based on single name cds
cds_constituent_member_default_count_pct = {
    'CDX EM': (3/22),
    'CDX IG': (1 / 125),
    'CDX HY': (6 / 100),
    'ITRAXX MAIN': (1 / 125),
    'ITRAXX XOVER': (6 / 75),
    'ITRAXX FINS SUB': (2 / 30),
    'ITRAXX FINS SNR': (2 / 30),
}

cds_loss_rate = {
    'CDX EM': 0.8,
    'CDX IG': 0.6,
    'CDX HY': 0.8,
    'ITRAXX MAIN': 0.6,
    'ITRAXX XOVER': 0.8,
    'ITRAXX FINS SUB': 0.6,
    'ITRAXX FINS SNR':  0.8,
}

direction_multiplier_payout ={
    'Buy Protection': 1,
    'Sell Protection': -1
}

def calculate_abs_net_after_default_carry(row, loss_side_only=None, row_file='pair_trade_file'):
    '''
    ['Index-1', 'Series-1', 'Att-Det ach 1', 'Tenor 1', 'Maturity 1', 'Trade 1','Quote 1',
    'Index-2', 'Series-2', 'Att-Detach 2', 'Tenor 2', 'Maturity 2', 'Trade 2','Quote 2',
    'Beta Ratio', 'Reason', 'Type', 'Percentile', 'T Cost',
    'Target_Return', 'Net Carry', 'Net 12m R+C', 'Net 12m % Rtn', 'Net Basis', 'Notional 1', 'Notional 2','Net Upfront']'''


    if row_file =='index_tranche_properties':
        duration_1, duration_2 = row['index_maturity_years'], 0
        trade_1, trade_2 = 'Sell Protection', 'Buy Protection'
        quote_1, quote_2  = row['bid'], 0
        direction_multiplier_payout_1  = -1 #sell protection
        direction_multiplier_payout_2 = 0
        size_1 = 10000 # convert back into into bps (since below uses actual notionals)
        size_2 = 0
        attachment_1, detachment_1 = row['attachment'], row['detachment']
        attachment_2, detachment_2 = 0 , 1
        index_1 , index_2 = row['index_short_name'], 'n/a'

    else: # 'pair_trade_file'
        duration_1, duration_2 = float(re.match(r'^[\d.]+', row['Maturity 1']).group()), float(re.match(r'^[\d.]+', row['Maturity 2']).group())
        trade_1, trade_2 = row['Trade 1'], row['Trade 2']
        quote_1, quote_2 = row['Quote 1'], row['Quote 2']
        direction_multiplier_payout_1, direction_multiplier_payout_2 = direction_multiplier_payout[row['Trade 1']], direction_multiplier_payout[row['Trade 2']]
        size_1, size_2  =row['Notional 1'], row['Notional 2']
        attachment_1, detachment_1 = map(float, row['Att-Detach 1'].split('-'))
        attachment_2, detachment_2 = map(float, row['Att-Detach 2'].split('-'))
        index_1, index_2 = row['Index-1'], row['Index-2']


    # add find net carry abs to maturity
    net_carry_to_maturity_abs = ((direction_multiplier_payout_1 * -1 * quote_1/10000 * size_1 * duration_1) +
                                 (direction_multiplier_payout_2 * -1 * quote_2/10000 * size_2 * duration_2))

    # risk reduction multiplier and expected loss
    expected_loss_rate_1 = cds_constituent_member_default_count_pct[index_1] * cds_loss_rate[index_1] * risk_extrapolation_to_duration(duration=duration_1)
    if index_2 == 'n/a':
        expected_loss_rate_2=0
    else:
        expected_loss_rate_2 = cds_constituent_member_default_count_pct[index_2] * cds_loss_rate[index_2] * risk_extrapolation_to_duration(duration=duration_2)

    # default multiplier
    default_multiplier_1 = np.where((expected_loss_rate_1 > attachment_1) & (expected_loss_rate_1 <= detachment_1),((expected_loss_rate_1 - attachment_1) / (detachment_1 - attachment_1)), 0)
    if index_2 == 'n/a':
        default_multiplier_2=0
    else:
        default_multiplier_2 = np.where((expected_loss_rate_2 > attachment_2) & (expected_loss_rate_2 <= detachment_2),((expected_loss_rate_2 - attachment_2) / (detachment_2 - attachment_2)), 0)

    payout_1 = direction_multiplier_payout[trade_1] * size_1 * default_multiplier_1
    payout_2 = direction_multiplier_payout[trade_2] * size_2 * default_multiplier_2

    if loss_side_only is not None:
        if payout_1 > 0:
            payout_1 = 0
        elif payout_2 > 0:
            payout_2 = 0

    net_pnl_maturity = net_carry_to_maturity_abs + payout_1 + payout_2
    return net_pnl_maturity

class calculate_portfolio_expected_defaults_by_index():
    def __init__(self,portfolio_df, current_tranche_index_properties):
        self.portfolio_df = portfolio_df
        self.current_tranche_index_properties = current_tranche_index_properties

        # IMPROVEMENT -> USE ACTUAL SINGLE NAME CDS QUOTES AND RECOVERY
        # IMPROVEMENT -> FIND THE ACTUAL DURATION AND ADJUST
        df = portfolio_df

        df1 = df[['Index-1','Series-1','Att-Detach 1','Trade 1','Notional 1']]
        df2 = df[['Index-2','Series-2', 'Att-Detach 2', 'Trade 2', 'Notional 2']]

        df1[['attachment_1','detachment_1']] = df1['Att-Detach 1'].str.split('-', expand=True)
        df2[['attachment_2', 'detachment_2']] = df2['Att-Detach 2'].str.split('-', expand=True)

        df1['attachment_1'], df1['detachment_1'] = df1['attachment_1'].astype(float), df1['detachment_1'].astype(float)
        df2['attachment_2'], df2['detachment_2'] = df2['attachment_2'].astype(float), df2['detachment_2'].astype(float)

        df1 = df1.merge(
            current_tranche_index_properties[['index_short_name', 'index_series', 'attachment', 'detachment', 'index_maturity_years']],
            left_on=['Index-1', 'Series-1', 'attachment_1', 'detachment_1'],
            right_on=['index_short_name', 'index_series', 'attachment', 'detachment'],
            how='left'
        )
        df1.drop(columns=['index_short_name', 'index_series', 'attachment', 'detachment'], inplace=True)

        df2 = df2.merge(
            current_tranche_index_properties[['index_short_name', 'index_series', 'attachment', 'detachment', 'index_maturity_years']],
            left_on=['Index-2', 'Series-2', 'attachment_2', 'detachment_2'],
            right_on=['index_short_name', 'index_series', 'attachment', 'detachment'],
            how='left'
        )
        df2.drop(columns=['index_short_name', 'index_series', 'attachment', 'detachment'], inplace=True)

        # find total default loss of index
        df1['index_multiplier'] = df1['Index-1'].map(cds_constituent_member_default_count_pct) * df1['Index-1'].map(cds_loss_rate) * df1.apply(lambda row: risk_extrapolation_to_duration(duration=row['index_maturity_years']), axis=1)
        df2['index_multiplier'] = df2['Index-2'].map(cds_constituent_member_default_count_pct) * df2['Index-2'].map(cds_loss_rate) * df2.apply(lambda row: risk_extrapolation_to_duration(duration=row['index_maturity_years']), axis=1)

        # find default loss of index and tranche
        df1['default_multiplier'] = np.where(
            (df1['index_multiplier'] > df1['attachment_1']) & (df1['index_multiplier'] <= df1['detachment_1']),
            ((df1['index_multiplier']-df1['attachment_1']) / (df1['detachment_1']-df1['attachment_1'])),0)
        # find default loss of index and tranche
        df2['default_multiplier'] = np.where(
            (df2['index_multiplier'] > df2['attachment_2']) & (df2['index_multiplier'] <= df2['detachment_2']),
            ((df2['index_multiplier']-df2['attachment_2']) / (df2['detachment_2']-df2['attachment_2'])),0)

        self.df1 = df1
        self.df2 = df2

    def expected_default_loss(self, grouped_by):
        df1 = self.df1
        df2 = self.df2
        # annualise the notional
        df1['Notional 1'] = df1.apply(lambda row: row['Notional 1']/row['index_maturity_years'] * -1 if row['Trade 1'] == 'Sell Protection' else row['Notional 1']/row['index_maturity_years'], axis=1)
        df2['Notional 2'] = df2.apply(lambda row: row['Notional 2']/row['index_maturity_years'] * -1 if row['Trade 2'] == 'Sell Protection' else row['Notional 2']/row['index_maturity_years'], axis=1)
        df1['annual default loss'] = df1['default_multiplier'] * df1['Notional 1']
        df2['annual default loss'] = df2['default_multiplier'] * df2['Notional 2']

        if grouped_by == 'sub_product':
            pass
            # to fill
        else: #index
            df1 = df1.rename(columns={'Index-1': 'index'})
            df2 = df2.rename(columns={'Index-2': 'index'})
            df1 = df1[['index', 'annual default loss']]
            df2 = df2[['index', 'annual default loss']]
            combined_df = pd.concat([df1, df2], ignore_index=True)
            final_df = combined_df.groupby('index').sum().reset_index()

        return final_df

    def hedged_notional_size_with_index(self, grouped_by):
        df1 = self.df1
        df2 = self.df2
        df1['index_hedge_notional'] = ((df1['default_multiplier'] * df1['Notional 1']) / df1['index_multiplier']) * (df1['Trade 1'].map(direction_multiplier_payout) * -1)
        df2['index_hedge_notional'] = ((df2['default_multiplier'] * df2['Notional 2']) / df2['index_multiplier']) * (df2['Trade 2'].map(direction_multiplier_payout) * -1)

        if grouped_by == 'sub_product':
            pass
            # to fill
        else: #index
            df1 = df1.rename(columns={'Index-1': 'index'})
            df2 = df2.rename(columns={'Index-2': 'index'})
            df1 = df1[['index', 'index_hedge_notional']]
            df2 = df2[['index', 'index_hedge_notional']]
            combined_df = pd.concat([df1, df2], ignore_index=True)
            final_df = combined_df.groupby('index').sum().reset_index()

        return final_df
