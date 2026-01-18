import variable_keys_beta
import pandas as pd
import sys
import os
import numpy as np
import math
import sympy as sp

directory = os.getcwd()
inputs_file_path = os.path.join(directory, 'inputs', 'inputs.xlsx')
liquidity_base_notional = pd.read_excel(inputs_file_path, sheet_name='liquidity_index_tranche')

#index_tranche_latest_versions_series_df
'''
index_short_name	notional

'''
#index_row
'''
('pricedate', '2025-02-03') ('index_short_name', 'CDX EM') ('ig_hy_em', 'EM') ('index_series', 36) ('index_coupon', 100.0)
 ('index_version', 3) ('index_maturity', Timestamp('2026-12-20 00:00:00')) ('tenor', '5Y') ('attachment', 0.0) ('detachment', 1.0)
'macro_product' , index_currency, 
'''


class index_tranche:
    def __init__(self,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        for column in self.df_row.index:
            setattr(self, column, self.df_row[column])

        # find latest series
        self.index_tranche_latest_versions_df=self.index_tranche_latest_versions_df[(self.index_tranche_latest_versions_df['pricedate']==self.pricedate) & (self.index_tranche_latest_versions_df['index_short_name']==self.index_short_name) & (self.index_tranche_latest_versions_df['macro_product']==self.macro_product)]
        self.max_series = self.index_tranche_latest_versions_df['index_series'].max()

        # base notional (all the notionals will feed from the below)
        # set base notional-> liquid current 5Yr Index
        if self.df_row['ig_hy_em'] == 'IG':
            self.base_notional = 300000000
        elif self.df_row['ig_hy_em'] == 'HY':
            self.base_notional = 50000000
        elif self.df_row['ig_hy_em'] == 'EM':
            self.base_notional = 50000000
        else:
            self.base_notional = 100000000

        # overwrite if there's a specific number set for the specific index
        if not liquidity_base_notional[liquidity_base_notional['index_short_name']==self.index_short_name]['notional'].empty:
            self.base_notional = liquidity_base_notional[liquidity_base_notional['index_short_name']==self.index_short_name]['notional'].values[0]

    def liquidity_calculator(self):

        # find product sub level
        sub_label_finder = self.index_short_name.lower().replace(' ','_') + '_' + 'index_tranche_sub_label'
        sub_label_df = eval(f'variable_keys_beta.{sub_label_finder}')
        sub_label = sub_label_df[(sub_label_df['attachment']==float(self.attachment)) & (sub_label_df['detachment']==float(self.detachment))]['sub_level'].values[0]

        # find new base_notional - index+series ages
        series_diff = abs(self.max_series - self.index_series)
        self.base_notional = ((3 * np.exp(-0.1 * series_diff)) - 2) * self.base_notional

        # find lowest base notional - older series that produce a negative value
        if self.base_notional <= 0:
            if self.df_row['ig_hy_em'] == 'IG':
                self.base_notional = 20000000
            elif self.df_row['ig_hy_em'] == 'HY':
                self.base_notional = 5000000
            elif self.df_row['ig_hy_em'] == 'EM':
                self.base_notional = 5000000
            else:
                self.base_notional = 5000000

        # using base notional -> find liquid notional given index or tranche level
        if sub_label =='index':
            liquid_notional = self.base_notional
        elif sub_label =='super senior':
            liquid_notional = (float(self.detachment) - float(self.attachment)) * self.base_notional
        elif sub_label == 'senior mezzanine':
            liquid_notional = (float(self.detachment) - float(self.attachment)) * self.base_notional
        elif sub_label == 'junior mezzanine':
            liquid_notional = (float(self.detachment) - float(self.attachment)) * self.base_notional
        elif sub_label == 'equity':
            liquid_notional = (float(self.detachment) - float(self.attachment)) * self.base_notional
        else:
            liquid_notional = 0.25 * self.base_notional

        # clean the notional (nearest 5 million)
        liquid_notional = math.ceil(liquid_notional/5000000) * 5000000

        int(liquid_notional)

        return liquid_notional

    @staticmethod
    def calculate_pair_notionals(index_liquid_notional_1, index_liquid_notional_2, beta_ratio_pair):
        # formula to maximise
            # 0<=x<=index_liquid_notional_1
            # 0<=y<=index_liquid_notional_2
            # y=x/beta_ratio_pair
        if (index_liquid_notional_2 * beta_ratio_pair) <= index_liquid_notional_1:
            y = index_liquid_notional_2
            x = y * beta_ratio_pair
        else:
            x = index_liquid_notional_1
            y = index_liquid_notional_1/beta_ratio_pair

        return round(x, -5), round(y, -5)





