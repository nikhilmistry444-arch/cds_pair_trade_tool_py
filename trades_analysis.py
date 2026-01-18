# Description: Main file to run the cds index and tranche tool
import cds_raw_data
import cds_momentum_signal
import cds_calculator
import fx_calculator
import results_evaluator
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
import tranche_delta_runs
import cds_raw_data_cache
import live_data
import results_evaluator
import default_analysis
import cds_margin_im_vm

import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import sys
import pickle
import os
import ast

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats


bbg_cds_ticker_key = cds_raw_data.cds_list()


class trades_analysis():

    def __init__(self,trades_df,current_tranche_index_properties, historic_quotes, interest_rates_swap_curves_df,fx_rates_df, end_date, fx_conversion, cds_constituents_df, cash_benchmarked, file_path):
        self.trades_df = trades_df
        self.current_tranche_index_properties = current_tranche_index_properties
        self.historic_quotes = historic_quotes
        self.interest_rates_swap_curves_df = interest_rates_swap_curves_df
        self.fx_rates_df = fx_rates_df
        self.end_date = end_date
        self.fx_conversion = fx_conversion
        self.cds_constituents_df = cds_constituents_df
        self.cash_benchmarked = cash_benchmarked
        self.file_path = file_path

        self.cds_defaults = cds_raw_data.cds_defaults()

        # cash usage
        self.cash_usage_carry = self.cash_benchmarked

    def calculate_pnl_rtn(row):
        if row['cash usage'] == 0:
            combined_cash_usage = 1
        else:
            combined_cash_usage = row['cash usage']

        if row['accrual_days_x'] == 0:
            row['accrual_days_x'] = 1

        if combined_cash_usage <= 0:
            if row['rolling_pnl'] > 0:
                return 100
            else:
                return round(((row['rolling_pnl'] * (1/(row['accrual_days_x'] / 365.25))) / combined_cash_usage) * 100, 0) * -1
        else:
            return round(((row['rolling_pnl'] * (1/(row['accrual_days_x'] / 365.25))) / combined_cash_usage) * 100, 0)

    def calculate_cumulative_default_loss(row,trade_date, direction_multiplier, cds_constituents_defaulted, attachment, detachment):

        # find total realised loss as of date
        sub_cds_constituents_defaulted = cds_constituents_defaulted[(cds_constituents_defaulted['default_date'] >= trade_date) & (cds_constituents_defaulted['default_date'] <= row['pricedate'])]
        sub_cds_constituents_defaulted['cds_loss'] = sub_cds_constituents_defaulted['cds_weight'] * sub_cds_constituents_defaulted['recovery_rate']
        product_total_loss = sub_cds_constituents_defaulted['cds_loss'].sum()
        # find loss in bps from total notional based on attachment and detachment
        if attachment <= product_total_loss <= detachment:
            product_actual_loss = product_total_loss - attachment
        else:
            product_actual_loss = 0
        bps_product_actual_loss = product_actual_loss / (detachment - attachment)
        default_loss_bps = bps_product_actual_loss * direction_multiplier * -1

        return default_loss_bps

    @staticmethod
    def calculate_cumulative_series(final_df,combined_df, trade_date):

        combined_df = combined_df[['pricedate', 'rolling_pnl', 'cr01', 'net upfront','cash usage']]
        if final_df.empty:
            daily_cumulative_pnl_series = combined_df[['pricedate','rolling_pnl','net upfront','cr01', 'cash usage']]
        else:
            outer_join_df = pd.merge(final_df, combined_df[['pricedate','rolling_pnl','net upfront','cr01', 'cash usage']], on='pricedate', how='outer', suffixes=('_df1', '_df2'))
            # Fill NaN values with 0 for quantity columns -
            outer_join_df['rolling_pnl_df1'] = outer_join_df['rolling_pnl_df1'].fillna(0)
            outer_join_df['rolling_pnl_df2'] = outer_join_df['rolling_pnl_df2'].fillna(0)
            outer_join_df['net upfront_df1'] = outer_join_df['net upfront_df1'].fillna(0)
            outer_join_df['net upfront_df2'] = outer_join_df['net upfront_df2'].fillna(0)
            outer_join_df['cr01_df1'] = outer_join_df['cr01_df1'].fillna(0)
            outer_join_df['cr01_df2'] = outer_join_df['cr01_df2'].fillna(0)
            outer_join_df['cash usage_df1'] = outer_join_df['cash usage_df1'].fillna(0)
            outer_join_df['cash usage_df2'] = outer_join_df['cash usage_df2'].fillna(0)

            # sum the tables together
            outer_join_df['rolling_pnl'] = round(outer_join_df['rolling_pnl_df1'] + outer_join_df['rolling_pnl_df2'],0)
            outer_join_df['net upfront'] = round(outer_join_df['net upfront_df1'] + outer_join_df['net upfront_df2'],0)
            outer_join_df['cr01'] = round(outer_join_df['cr01_df1'] + outer_join_df['cr01_df2'],0)
            outer_join_df['cash usage'] = round(outer_join_df['cash usage_df1'] + outer_join_df['cash usage_df2'], 0)
            daily_cumulative_pnl_series = outer_join_df[['pricedate','rolling_pnl', 'net upfront','cr01', 'cash usage']]

        daily_cumulative_pnl_series = daily_cumulative_pnl_series.sort_values(by='pricedate')

        return daily_cumulative_pnl_series

    @staticmethod
    def calculate_cumulative_series_hybrid(final_df,combined_df,target_return, net_carry_maturity ,trade_date):
        combined_df = combined_df[['pricedate', 'rolling_pnl', 'cr01','net upfront', 'cash usage']]

        # below only different from the closed trades function
        earliest_close_trade_date = combined_df[combined_df['rolling_pnl'] >= max(target_return, net_carry_maturity)]['pricedate'].min()

        if pd.isna(earliest_close_trade_date):
            print('never max(target return, net_carry_maturity)')
        else:
            pnl = combined_df[combined_df['pricedate'] == earliest_close_trade_date]['rolling_pnl'].values[0]
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'rolling_pnl'] = pnl
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'cr01'] = 0
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'net upfront'] = 0
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'cash usage'] = 0
        if final_df.empty:
            daily_cumulative_pnl_series = combined_df[['pricedate','rolling_pnl','net upfront','cr01', 'cash usage']]
        else:
            outer_join_df = pd.merge(final_df, combined_df[['pricedate','rolling_pnl','net upfront','cr01','cash usage']], on='pricedate', how='outer', suffixes=('_df1', '_df2'))

            # assign pnl that's locked in/matured trades and keep thereon after - df1
            i=0
            for i in range(len(outer_join_df['pricedate'])):
                if pd.isna(outer_join_df.loc[i,'rolling_pnl_df1']):
                    if i == 0:
                        outer_join_df.loc[i, 'rolling_pnl_df1'] = 0
                        outer_join_df.loc[i, 'cr01_df1'] = 0
                        outer_join_df.loc[i, 'net upfront_df1'] = 0
                        outer_join_df.loc[i, 'cash usage_df1'] = 0
                    else:
                        outer_join_df.loc[i, 'rolling_pnl_df1'] = outer_join_df.loc[i-1, 'rolling_pnl_df1']
                        outer_join_df.loc[i, 'cr01_df1'] = outer_join_df.loc[i - 1, 'cr01_df1']
                        outer_join_df.loc[i, 'net upfront_df1'] = outer_join_df.loc[i - 1, 'net upfront_df1']
                        outer_join_df.loc[i, 'cash usage_df1'] = outer_join_df.loc[i - 1, 'cash usage_df1']

                if pd.isna(outer_join_df.loc[i,'rolling_pnl_df2']):
                    if i == 0:
                        outer_join_df.loc[i, 'rolling_pnl_df2'] = 0
                        outer_join_df.loc[i, 'cr01_df2'] = 0
                        outer_join_df.loc[i, 'net upfront_df2'] = 0
                        outer_join_df.loc[i, 'cash usage_df2'] = 0
                    else:
                        outer_join_df.loc[i, 'rolling_pnl_df2'] = outer_join_df.loc[i-1, 'rolling_pnl_df2']
                        outer_join_df.loc[i, 'cr01_df2'] = outer_join_df.loc[i - 1, 'cr01_df2']
                        outer_join_df.loc[i, 'net upfront_df2'] = outer_join_df.loc[i - 1, 'net upfront_df2']
                        outer_join_df.loc[i, 'cash usage_df2'] = outer_join_df.loc[i - 1, 'cash usage_df2']

            # Fill NaN values with 0 for quantity columns
            outer_join_df['net upfront_df1'] = outer_join_df['net upfront_df1'].fillna(0)
            outer_join_df['net upfront_df2'] = outer_join_df['net upfront_df2'].fillna(0)
            outer_join_df['cr01_df1'] = outer_join_df['cr01_df1'].fillna(0)
            outer_join_df['cr01_df2'] = outer_join_df['cr01_df2'].fillna(0)
            outer_join_df['cash usage_df1'] = outer_join_df['cash usage_df1'].fillna(0)
            outer_join_df['cash usage_df2'] = outer_join_df['cash usage_df2'].fillna(0)

            # sum the tables together
            outer_join_df['rolling_pnl'] = round(outer_join_df['rolling_pnl_df1'] + outer_join_df['rolling_pnl_df2'],0)
            outer_join_df['net upfront'] = round(outer_join_df['net upfront_df1'] + outer_join_df['net upfront_df2'],0)
            outer_join_df['cr01'] = round(outer_join_df['cr01_df1'] + outer_join_df['cr01_df2'],0)
            outer_join_df['cash usage'] = round(outer_join_df['cash usage_df1'] + outer_join_df['cash usage_df2'], 0)
            daily_cumulative_pnl_series = outer_join_df[['pricedate','rolling_pnl', 'net upfront','cr01','cash usage']]

        daily_cumulative_pnl_series = daily_cumulative_pnl_series.sort_values(by='pricedate')

        return daily_cumulative_pnl_series

    @staticmethod
    def calculate_cumulative_series_close_trades(final_df,combined_df,target_return, trade_date):

        combined_df = combined_df[['pricedate', 'rolling_pnl', 'cr01','net upfront', 'cash usage']]
        earliest_close_trade_date = combined_df[combined_df['rolling_pnl'] >= target_return]['pricedate'].min()
        if pd.isna(earliest_close_trade_date):
            print('never reaches target return')
        else:
            pnl = combined_df[combined_df['pricedate'] == earliest_close_trade_date]['rolling_pnl'].values[0]
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'rolling_pnl'] = pnl
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'cr01'] = 0
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'net upfront'] = 0
            combined_df.loc[combined_df['pricedate'] > earliest_close_trade_date, 'cash usage'] = 0
        if final_df.empty:
            daily_cumulative_pnl_series = combined_df[['pricedate','rolling_pnl','net upfront','cr01','cash usage']]
        else:
            outer_join_df = pd.merge(final_df, combined_df[['pricedate','rolling_pnl','net upfront','cr01','cash usage']], on='pricedate', how='outer', suffixes=('_df1', '_df2'))

            # assign pnl that's locked in/matured trades and keep thereon after - df1
            i=0
            for i in range(len(outer_join_df['pricedate'])):
                if pd.isna(outer_join_df.loc[i,'rolling_pnl_df1']):
                    if i == 0:
                        outer_join_df.loc[i, 'rolling_pnl_df1'] = 0
                        outer_join_df.loc[i, 'cr01_df1'] = 0
                        outer_join_df.loc[i, 'net upfront_df1'] = 0
                        outer_join_df.loc[i, 'cash usage_df1'] = 0
                    else:
                        outer_join_df.loc[i, 'rolling_pnl_df1'] = outer_join_df.loc[i-1, 'rolling_pnl_df1']
                        outer_join_df.loc[i, 'cr01_df1'] = outer_join_df.loc[i - 1, 'cr01_df1']
                        outer_join_df.loc[i, 'net upfront_df1'] = outer_join_df.loc[i - 1, 'net upfront_df1']
                        outer_join_df.loc[i, 'cash usage_df1'] = outer_join_df.loc[i - 1, 'cash usage_df1']

                if pd.isna(outer_join_df.loc[i,'rolling_pnl_df2']):
                    if i == 0:
                        outer_join_df.loc[i, 'rolling_pnl_df2'] = 0
                        outer_join_df.loc[i, 'cr01_df2'] = 0
                        outer_join_df.loc[i, 'net upfront_df2'] = 0
                        outer_join_df.loc[i, 'cash usage_df2'] = 0
                    else:
                        outer_join_df.loc[i, 'rolling_pnl_df2'] = outer_join_df.loc[i-1, 'rolling_pnl_df2']
                        outer_join_df.loc[i, 'cr01_df2'] = outer_join_df.loc[i - 1, 'cr01_df2']
                        outer_join_df.loc[i, 'net upfront_df2'] = outer_join_df.loc[i - 1, 'net upfront_df2']
                        outer_join_df.loc[i, 'cash usage_df2'] = outer_join_df.loc[i - 1, 'cash usage_df2']

            # Fill NaN values with 0 for quantity columns
            outer_join_df['net upfront_df1'] = outer_join_df['net upfront_df1'].fillna(0)
            outer_join_df['net upfront_df2'] = outer_join_df['net upfront_df2'].fillna(0)
            outer_join_df['cr01_df1'] = outer_join_df['cr01_df1'].fillna(0)
            outer_join_df['cr01_df2'] = outer_join_df['cr01_df2'].fillna(0)
            outer_join_df['cash usage_df1'] = outer_join_df['cash usage_df1'].fillna(0)
            outer_join_df['cash usage_df2'] = outer_join_df['cash usage_df2'].fillna(0)

            # sum the tables together
            outer_join_df['rolling_pnl'] = round(outer_join_df['rolling_pnl_df1'] + outer_join_df['rolling_pnl_df2'],0)
            outer_join_df['net upfront'] = round(outer_join_df['net upfront_df1'] + outer_join_df['net upfront_df2'],0)
            outer_join_df['cr01'] = round(outer_join_df['cr01_df1'] + outer_join_df['cr01_df2'],0)
            outer_join_df['cash usage'] = round(outer_join_df['cash usage_df1'] + outer_join_df['cash usage_df2'], 0)
            daily_cumulative_pnl_series = outer_join_df[['pricedate','rolling_pnl', 'net upfront','cr01','cash usage']]

        daily_cumulative_pnl_series = daily_cumulative_pnl_series.sort_values(by='pricedate')

        return daily_cumulative_pnl_series

    @staticmethod
    def calculate_exposure_by_index_name(exposures_by_index_name_df, row, index_1_index_maturity_year, index_2_index_maturity_year,direction_multiplier_1,direction_multiplier_2,index_1_upfront_amount, index_2_upfront_amount, index_1_cash_usage, index_2_cash_usage):
        # -> delta adjusted cr01

        duration_bins = [0,2,4,6,9,11]
        duration_labels = ['0-2', '2-4', '4-6','6-9', '9-11']
        tranche_delta_df = tranche_delta_runs.tranche_deltas()
        locals()[f'index_1_index_maturity_year'], locals()[f'index_2_index_maturity_year'] = index_1_index_maturity_year, index_2_index_maturity_year
        locals()[f'direction_multiplier_1'], locals()[f'direction_multiplier_2'] = direction_multiplier_1, direction_multiplier_2
        locals()[f'index_1_upfront_amount'], locals()[f'index_2_upfront_amount'] = index_1_upfront_amount, index_2_upfront_amount

        # convert attach_detach (ie 0.03-0.06 to 3-6) and delta adjust cr01
        for z in [1,2]:
            if locals()[f'index_{z}_index_maturity_year'] > 0: # keep live trades that haven't matured
                locals()[f'attachment_{z}'], locals()[f'detachment_{z}'] = int(float(row['Att-Detach '+str(z)].split('-')[0])*100), int(float(row['Att-Detach '+str(z)].split('-')[1])*100)
                locals()[f'attach_detach_{z}'] = str(locals()[f'attachment_{z}']) + str('-') + str(locals()[f'detachment_{z}'])
                attach_detach = locals()[f'attach_detach_{z}']
                if (locals()[f'attachment_{z}'] == 0 and locals()[f'detachment_{z}'] == 100):
                    locals()[f'delta_risk_multiplier_{z}'] = 1
                else:
                    dictionary_finder = str(row['Index-'+str(z)].replace(' ', '_')) + str('_') + str(5)
                    bbg_cds_ticker = str(variable_keys_beta.cds_index_bbg_core_ticker[row['Index-'+str(z)]]) + str(5) + str(row['Series-'+str(z)])
                    try:
                        locals()[f'delta_risk_multiplier_{z}'] = tranche_delta_df[(tranche_delta_df['index_short_name_generic'] == dictionary_finder) & (tranche_delta_df['bbg_index_number'] == bbg_cds_ticker) & (tranche_delta_df['attachment-detachment'] == attach_detach)]['delta'].values[0]
                    except:
                        locals()[f'delta_risk_multiplier_{z}'] = tranche_delta_runs.backup_tranche_deltas(df=tranche_delta_df, dictionary_finder=dictionary_finder, attachment_detachment=attach_detach, current_series=row['Series-1'])

                # create dataframe from the pairs to evaluate
                locals()[f'df{z}'] = pd.DataFrame({
                                        'index': [row['Index-'+str(z)]],
                                        'maturity': [locals()[f'index_{z}_index_maturity_year']],
                                        'cr01': [int((locals()[f'index_{z}_index_maturity_year'] / 10000) * row['Notional '+str(z)] * locals()[f'delta_risk_multiplier_{z}'] * -1 * locals()[f'direction_multiplier_{z}'])],
                                        'carry': [int((row['Quote '+str(z)] / 10000) * row['Notional '+str(z)] * locals()[f'direction_multiplier_{z}'])],
                                        'upfront': [int(locals()[f'index_{z}_upfront_amount'])],
                                        'cash usage': [int(locals()[f'index_{z}_cash_usage'])]
                })
            else:
                locals()[f'df{z}'] = pd.DataFrame(columns=['index','maturity','cr01','carry','upfront', 'cash usage'])

        pre_combined_df = pd.concat([locals()['df1'], locals()['df2']], ignore_index=True)
        try:
            pre_combined_df['maturity'] = pd.cut(pre_combined_df['maturity'], bins=duration_bins, labels=duration_labels)
        except:
            pre_combined_df = pd.DataFrame(columns=['index', 'maturity', 'cr01', 'carry', 'upfront', 'cash usage'])

        if exposures_by_index_name_df.empty:
            exposures_by_index_name_df = pre_combined_df
        else:
            exposures_by_index_name_df = pd.concat([pre_combined_df, exposures_by_index_name_df], ignore_index=True)

        return exposures_by_index_name_df

    @staticmethod
    def calculate_exposure_by_ticker(exposures_by_ticker_df,current_tranche_index_properties , row, index_1_index_maturity_year, index_2_index_maturity_year, cds_constituents_1, cds_constituents_2, direction_multiplier_1, direction_multiplier_2):

        locals()[f'index_1_index_maturity_year'], locals()[f'index_2_index_maturity_year'] = index_1_index_maturity_year, index_2_index_maturity_year
        locals()[f'cds_constituents_1'], locals()[f'cds_constituents_2'] = cds_constituents_1, cds_constituents_2
        locals()[f'direction_multiplier_1'], locals()[f'direction_multiplier_2'] = direction_multiplier_1, direction_multiplier_2

        for z in [1,2]:
            if locals()[f'index_{z}_index_maturity_year'] > 0: # keep live trades that havent matured

                locals()[f'attachment_{z}'], locals()[f'detachment_{z}'] = float(row['Att-Detach '+str(z)].split('-')[0]), float(row['Att-Detach '+str(z)].split('-')[1])
                #cds constituents
                if (locals()[f'attachment_{z}']==0 and locals()[f'detachment_{z}']==1):
                    locals()[f'cds_constituents_{z}'] = locals()[f'cds_constituents_{z}'][['cds_constituents', 'cds_weight']]
                else:
                    try:
                        locals()[f'basis_list_{z}'] = current_tranche_index_properties[(current_tranche_index_properties['index_short_name'] == row['Index-'+str(z)])
                                                                    & (current_tranche_index_properties['index_series'] == row['Series-'+str(z)])
                                                                    & (current_tranche_index_properties['attachment'] == locals()[f'attachment_{z}'])
                                                                    & (current_tranche_index_properties['detachment'] == locals()[f'detachment_{z}'])]['basis_hedges'].values[0]
                    except:
                        continue # most likely the product has matured or is too old. so index error
                    if locals()[f'basis_list_{z}'] == '': #there is no relevant CDS for the tranche. With recoveries, it attach-detach is never reached
                        continue

                    locals()[f'basis_list_{z}'] = ast.literal_eval(locals()[f'basis_list_{z}'])
                    locals()[f'extracted_basis_list_{z}'] = [value.strip('()').split('-') for value in locals()[f'basis_list_{z}']]
                    locals()[f'cds_constituents_{z}'] = pd.DataFrame(locals()[f'extracted_basis_list_{z}'], columns=['cds_weight', 'cds_constituents'])
                    locals()[f'cds_constituents_{z}']['cds_weight'] = locals()[f'cds_constituents_{z}']['cds_weight'].astype(float)
                    locals()[f'cds_constituents_{z}']['cds_weight'] = locals()[f'cds_constituents_{z}']['cds_weight'] / 100
                locals()[f'cds_constituents_{z}']['notional'] = locals()[f'cds_constituents_{z}']['cds_weight'] * row['Notional '+str(z)] * -1 * locals()[f'direction_multiplier_{z}']
                locals()[f'cds_constituents_{z}']['cr01'] = locals()[f'cds_constituents_{z}']['notional'] * (locals()[f'index_{z}_index_maturity_year'] / 10000)

            else: #exclude matured products
                locals()[f'cds_constituents_{z}'] = pd.DataFrame(columns=['cds_constituents', 'cds_weight', 'notional','cr01'])

        combined_df = pd.concat([locals()['cds_constituents_1'], locals()['cds_constituents_2']], ignore_index=True)

        if exposures_by_ticker_df.empty:
            exposures_by_ticker_df = combined_df
        else:
            exposures_by_ticker_df = pd.concat([exposures_by_ticker_df, combined_df], ignore_index=True)

        # pivot combine the data
        exposures_by_ticker_df = pd.pivot_table(exposures_by_ticker_df, values=['notional','cr01'], index='cds_constituents', aggfunc='sum')
        exposures_by_ticker_df['notional'] = exposures_by_ticker_df['notional'].round(0)
        exposures_by_ticker_df['cr01'] = exposures_by_ticker_df['cr01'].round(0)
        exposures_by_ticker_df.reset_index(inplace=True)
        return exposures_by_ticker_df

    def produce_dataframe_and_calculate_pnl(self):
        '''Trade Date	Index-1	Series-1	Att-Detach 1	Tenor 1	Trade 1	Index-2	Series-2	Att-Detach 2	Tenor 2	Trade 2
        Type	Beta Ratio	Net Carry	Target_Return	Quote 1	Quote 2 Ctpy Notional 1 Notional 2 '''

        #### TO DO ####
        # add date key to left join. so when products mature. keep pnl as is


        daily_cumulative_pnl_pairs_df = pd.DataFrame()
        daily_cumulative_pnl_long_risk_df = pd.DataFrame()
        daily_cumulative_pnl_close_trades_df = pd.DataFrame()
        daily_cumulative_pnl_hybrid_df = pd.DataFrame()
        exposures_by_index_name_df = pd.DataFrame()   # ADD FX EXPOSURES TO UPFRONTS
        exposures_by_ticker_df = pd.DataFrame()

        for index_0, row_0 in self.trades_df.iterrows():
            e=None
            if pd.isnull(row_0['Trade Date']):
                break
            try:
                trade_date = row_0['Trade Date'].strftime('%Y-%m-%d')
                trade_date_datetime = pd.to_datetime(trade_date)

                # create variables
                # notionals are beta adjusted
                for z in [1,2]:
                    locals()[f'attachment_{z}'] , locals()[f'detachment_{z}'] = row_0['Att-Detach ' + str(z)].split('-')
                    locals()[f'attachment_{z}'], locals()[f'detachment_{z}'] = float(locals()[f'attachment_{z}']) , float(locals()[f'detachment_{z}'])
                    locals()[f'direction_multiplier_{z}'] = -1 if row_0['Trade '+str(z)] == 'Buy Protection' else 1

                    locals()[f'cds_constituents_{z}'] = self.cds_constituents_df[(self.cds_constituents_df['index_short_name'] ==row_0['Index-'+str(z)]) & (self.cds_constituents_df['index_series'] ==row_0['Series-'+str(z)])]
                    locals()[f'cds_constituents_total_weight_{z}'] = len(locals()[f'cds_constituents_{z}']['index_short_name'])
                    locals()[f'cds_constituents_defaulted_{z}'] = self.cds_defaults[self.cds_defaults['bbg_cds_ticker'].isin(locals()[f'cds_constituents_{z}']['cds_constituents'])]
                    try:
                        locals()[f'cds_constituents_defaulted_{z}']['cds_weight'] = 1 / locals()[f'cds_constituents_total_weight_{z}']
                    except:
                        locals()[f'cds_constituents_defaulted_{z}']['cds_weight'] = 1 / 100

                    # find historical quotes + assign cumulative loss
                    locals()[f'product_{z}_quotes_df'] = self.historic_quotes[(self.historic_quotes['index_short_name'] == row_0['Index-'+str(z)]) &
                                                            (self.historic_quotes['index_series'] == row_0['Series-'+str(z)]) &
                                                            (self.historic_quotes['tenor'] == row_0['Tenor '+str(z)]) &
                                                            (self.historic_quotes['attachment'] == locals()[f'attachment_{z}']) &
                                                            (self.historic_quotes['detachment'] == locals()[f'detachment_{z}']) &
                                                            (self.historic_quotes['pricedate'] >= trade_date)]
                    locals()[f'product_{z}_quotes_df'] = locals()[f'product_{z}_quotes_df'].sort_values(by='pricedate').reset_index(drop=True)

                    # clean the data. has funny spread quotes <0 like -100
                    # clean quotes that have snapped strangely or funnily (ie, -100 not the actual).
                    for i in range(len(locals()[f'product_{z}_quotes_df']['spread_quote'])):
                        if locals()[f'product_{z}_quotes_df'].loc[i, 'spread_quote'] < 0:
                            if i == 0:
                                j=i
                                while locals()[f'product_{z}_quotes_df'].loc[j, 'spread_quote'] in ['', np.nan] or locals()[f'product_{z}_quotes_df'].loc[j, 'spread_quote'] < 0:
                                    j = j + 1
                            else:
                                j = i
                                while locals()[f'product_{z}_quotes_df'].loc[j, 'spread_quote'] in ['', np.nan] or locals()[f'product_{z}_quotes_df'].loc[j, 'spread_quote'] < 0:
                                    j = j - 1
                            locals()[f'product_{z}_quotes_df'].loc[i, 'spread_quote'] = locals()[f'product_{z}_quotes_df'].loc[j, 'spread_quote']

                    locals()[f'product_{z}_quotes_df']['pricedate_dt'] = pd.to_datetime(locals()[f'product_{z}_quotes_df']['pricedate'])
                    locals()[f'product_{z}_quotes_df']['index_maturity_years'] = (pd.to_datetime(locals()[f'product_{z}_quotes_df']['index_maturity']) - pd.to_datetime(locals()[f'product_{z}_quotes_df']['pricedate'])).dt.days / 365.25
                    # details of the product as of the 'trade date' (trade is live then)
                    locals()[f'index_{z}_coupon'] = locals()[f'product_{z}_quotes_df']['index_coupon'].values[0]
                    locals()[f'index_{z}_currency'] = locals()[f'product_{z}_quotes_df']['index_currency'].values[0]
                    locals()[f'index_{z}_traded_fx_rate'] = fx_calculator.fx_rate_on_date(ccy=variable_keys_beta.cds_index_currency_key[row_0['Index-'+str(z)]], fx_conversion=self.fx_conversion, fx_rates_df=self.fx_rates_df, date=self.end_date)
                    locals()[f'index_{z}_index_maturity_year_then'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate_dt'].idxmin(), 'index_maturity_years']  #starting maturity years on trade date
                    locals()[f'index_{z}_index_maturity_year'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate_dt'].idxmax(), 'index_maturity_years'] #current maturity years on todays date
                    locals()[f'index_{z}_spread_quote'] = locals()[f'product_{z}_quotes_df']['spread_quote'].values[0]

                    # add fx data
                    locals()[f'product_{z}_quotes_df'] = fx_calculator.fx_column_trades_analysis(trade_date=trade_date, df=locals()[f'product_{z}_quotes_df'], fx_ccy=locals()[f'index_{z}_currency'], fx_conversion=self.fx_conversion, fx_rates_df=self.fx_rates_df)

                    try:
                        locals()[f'index_{z}_upfront_amount'] = row_0['Notional '+str(z)] * locals()[f'direction_multiplier_{z}'] * (1/10000) * cds_calculator.cds_upfront_calculator(swap_curve_df=self.interest_rates_swap_curves_df, currency=locals()[f'index_{z}_currency'], maturity_years=locals()[f'index_{z}_index_maturity_year_then'] , coupon=locals()[f'index_{z}_coupon'] - locals()[f'index_{z}_spread_quote'] , end_date=self.end_date) * locals()[f'index_{z}_traded_fx_rate']
                    except:
                        print('issue with upfront')
                        locals()[f'index_{z}_upfront_amount'] = row_0['Notional '+str(z)] * 100 * (1/10000) * locals()[f'direction_multiplier_{z}'] * locals()[f'index_{z}_traded_fx_rate'] # check this!

                    locals()[f'product_{z}_quotes_df']['upfront'] = locals()[f'index_{z}_upfront_amount']
                    # define variables from local scope to access into other functions
                    index_upfront_amount,direction_multiplier,cds_constituents_defaulted,attachment,detachment=locals()[f'index_{z}_upfront_amount'], locals()[f'direction_multiplier_{z}'], locals()[f'cds_constituents_defaulted_{z}'], locals()[f'attachment_{z}'], locals()[f'detachment_{z}']

                    # process the quotes
                        # find cumulative loss from trade date to now
                    locals()[f'product_{z}_quotes_df']['default_loss'] = locals()[f'product_{z}_quotes_df'].apply(lambda row: trades_analysis.calculate_cumulative_default_loss(row=row,
                                                                                                                                                                                trade_date=trade_date,
                                                                                                                                                                                direction_multiplier=direction_multiplier,
                                                                                                                                                                                cds_constituents_defaulted=cds_constituents_defaulted,
                                                                                                                                                                                attachment=attachment,
                                                                                                                                                                                detachment=detachment), axis=1)
                    locals()[f'product_{z}_quotes_df']['default_loss'] = locals()[f'product_{z}_quotes_df']['default_loss'] * row_0['Notional '+str(z)] * (1/10000) * locals()[f'index_{z}_traded_fx_rate']
                        # find accrual and spread change and pnl . fx adjusted
                    locals()[f'product_{z}_quotes_df']['pricedate_dt'] = pd.to_datetime(locals()[f'product_{z}_quotes_df']['pricedate'])
                    locals()[f'product_{z}_quotes_df']['accrual_days'] = locals()[f'product_{z}_quotes_df']['pricedate_dt'].apply(lambda x: (x - trade_date_datetime).days)
                    locals()[f'product_{z}_quotes_df']['duration'] = ((locals()[f'product_{z}_quotes_df']['index_maturity'] - locals()[f'product_{z}_quotes_df']['pricedate_dt']).dt.days / 365.25) * (direction_multiplier * -1)
                    locals()[f'product_{z}_quotes_df']['cr01'] = locals()[f'product_{z}_quotes_df']['duration'] * row_0['Notional '+str(z)] * (1/10000) * locals()[f'product_{z}_quotes_df']['fx_rate_daily']

                    locals()[f'product_{z}_quotes_df']['cash_usage_carry'] = locals()[f'product_{z}_quotes_df'].apply(lambda row: ((row['accrual_days']/365.25) * (self.cash_usage_carry/10000 * index_upfront_amount)) * -1, axis=1)
                    locals()[f'product_{z}_quotes_df']['rolldown_carry'] = locals()[f'product_{z}_quotes_df'].apply(lambda row: ((((row['accrual_days']/365.25 * row_0['Quote '+str(z)] * direction_multiplier)/10000) * row_0['Notional '+str(z)]) +
                                                                                                                                ((row['duration'] * (row['spread_quote']-row_0['Quote '+str(z)])) * row_0['Notional '+str(z)] * (1/10000)) *
                                                                                                                                 row['fx_rate_daily']),
                                                                                                                    axis=1)

                    # NOTE -> VALUES ARE AS OF NOTIONALS (BETA ADJUSTED) except for Duration

                    # dealing with when the product matures->zero out when product reaches maturity
                    index_closest_to_zero = locals()[f'product_{z}_quotes_df'][locals()[f'product_{z}_quotes_df']['index_maturity_years'] >= 0]['index_maturity_years'].idxmin()
                    pricedate_closest_to_zero = locals()[f'product_{z}_quotes_df'].loc[index_closest_to_zero,'pricedate']
                    # zero out
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'cr01'] = 0
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'upfront'] = 0
                    # keep last cumulative pnl value onwards
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'cash_usage_carry'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] == pricedate_closest_to_zero, 'cash_usage_carry'].values[0]
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'default_loss'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] == pricedate_closest_to_zero, 'default_loss'].values[0]
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'rolldown_carry'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] == pricedate_closest_to_zero, 'rolldown_carry'].values[0]
                    # finding the margin (recompute the cash_usage_carry)
                    locals()[f'product_{z}_quotes_df'] = cds_margin_im_vm.compute_daily_vm_im_margin_macro(df=locals()[f'product_{z}_quotes_df'], traded_spread=row_0['Quote '+str(z)], b_s_protection=row_0['Trade '+str(z)])
                    locals()[f'product_{z}_quotes_df']['cash_usage'] = locals()[f'product_{z}_quotes_df']['upfront'] + locals()[f'product_{z}_quotes_df']['margin']
                    locals()[f'product_{z}_quotes_df']['daily_margin_cost'] = locals()[f'product_{z}_quotes_df']['margin'] * (self.cash_usage_carry/10000) * (1/365.25)
                    locals()[f'product_{z}_quotes_df']['cumulative_margin_cost'] = locals()[f'product_{z}_quotes_df']['daily_margin_cost'].cumsum()
                    locals()[f'product_{z}_quotes_df']['cash_usage_carry'] = locals()[f'product_{z}_quotes_df']['cash_usage_carry'] + locals()[f'product_{z}_quotes_df']['cumulative_margin_cost']
                    locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] > pricedate_closest_to_zero, 'cash_usage_carry'] = locals()[f'product_{z}_quotes_df'].loc[locals()[f'product_{z}_quotes_df']['pricedate'] == pricedate_closest_to_zero, 'cash_usage_carry'].values[0]


                # net upfront (bps)
                combined_upfront = locals()['index_1_upfront_amount'] + (locals()['index_2_upfront_amount'])

                # combined quotes df
                #   -> assign max, min, current pnl(days elapsed) to main trades table and assign beta ratio
                #   -> when one leg matures. copy over from row before
                combined_quotes_df = locals()['product_1_quotes_df'].merge(locals()['product_2_quotes_df'], on=['pricedate'], how='outer', suffixes=('_x','_y'))
                for i in range(len(combined_quotes_df['pricedate'])):
                    for j in range(len(combined_quotes_df.columns)):
                        if pd.isna(combined_quotes_df.iloc[i, j]):
                            if i==0:
                                combined_quotes_df.iloc[i, j] = 0
                            else:
                                combined_quotes_df.iloc[i, j] = combined_quotes_df.iloc[i-1, j]

                combined_quotes_df['duration'] = combined_quotes_df['duration_x'] + (combined_quotes_df['duration_y']/row_0['Beta Ratio'])
                combined_quotes_df['cr01'] = combined_quotes_df['cr01_x'] + combined_quotes_df['cr01_y']
                combined_quotes_df['net upfront'] = combined_quotes_df['upfront_x'] + combined_quotes_df['upfront_y']
                combined_quotes_df['cash usage'] = combined_quotes_df['cash_usage_x'] + combined_quotes_df['cash_usage_y']
                combined_quotes_df['rolling_pnl_1'] = combined_quotes_df['default_loss_x'] + combined_quotes_df['rolldown_carry_x'] + combined_quotes_df['cash_usage_carry_x']
                combined_quotes_df['rolling_pnl_2'] = combined_quotes_df['default_loss_y'] + combined_quotes_df['rolldown_carry_y'] + combined_quotes_df['cash_usage_carry_x']
                combined_quotes_df['rolling_pnl'] = combined_quotes_df['rolling_pnl_1'] + combined_quotes_df['rolling_pnl_2']
                combined_quotes_df['pnl_rtn'] = combined_quotes_df.apply(lambda row: trades_analysis.calculate_pnl_rtn(row=row), axis=1)

                # finding max pnl
                max_for_accrual = combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmax(),'accrual_days_x']
                max_pnl = round(combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmax(),'rolling_pnl'],0)
                max_pnl_rtn = round(combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmax(),'pnl_rtn'],1)

                # finding min pnl
                min_for_accrual = combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmin(), 'accrual_days_x']
                min_pnl = round(combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmin(), 'rolling_pnl'],0)
                min_pnl_rtn = round(combined_quotes_df.loc[combined_quotes_df['rolling_pnl'].idxmin(), 'pnl_rtn'], 1)

                # current pnl
                #note, there may be series that have matured
                most_recent_date = min(combined_quotes_df['pricedate_dt_x'].max(), combined_quotes_df['pricedate_dt_y'].max())
                current_pnl_row = combined_quotes_df.loc[combined_quotes_df['pricedate'] == most_recent_date.strftime('%Y-%m-%d')]
                current_for_accrual = current_pnl_row['accrual_days_x'].values[0]
                current_pnl = round(current_pnl_row['rolling_pnl'].values[0], 0)
                current_pnl_rtn = round(current_pnl_row['pnl_rtn'].values[0], 1)
                current_combined_upfront = current_pnl_row['net upfront'].values[0]
                current_cash_usage_1, current_cash_usage_2 = current_pnl_row['cash_usage_x'].values[0] , current_pnl_row['cash_usage_y'].values[0]
                current_combined_cash_usage = current_pnl_row['cash usage'].values[0]

                # profitable returns
                pos_pnl_count = (combined_quotes_df['pnl_rtn'] > 0).sum()
                total_pnl_count = combined_quotes_df['pnl_rtn'].count()
                percentage_of_pnl = round((pos_pnl_count/total_pnl_count) * 100,0)

                # absolute pnl from target return, trades held to maturity on both legs
                target_return = row_0['Target_Return']
                target_return_abs = (row_0['Target_Return']/10000) * row_0['Notional 1']
                net_carry_abs = (row_0['Net Carry']/10000) * row_0['Notional 1']
                net_carry_to_maturity_abs = (locals()[f'direction_multiplier_1'] * row_0['Quote 1']/10000 * row_0['Notional 1'] * locals()['index_1_index_maturity_year_then']) + (locals()[f'direction_multiplier_2'] * row_0['Quote 2']/10000 * row_0['Notional 2'] * locals()['index_2_index_maturity_year_then'])

                # fill pair trade dataframe
                self.trades_df.loc[index_0, 'Net Upfront'] = round(current_combined_upfront, 0)
                self.trades_df.loc[index_0, 'cash usage'] = round(current_combined_cash_usage, 0)
                self.trades_df.loc[index_0, 'max_pnl'] = str(f"{int(max_pnl):,}") + str(' [') + str(max_for_accrual) + str('d]') + str(' [') + str(max_pnl_rtn) + str('%]')
                self.trades_df.loc[index_0, 'min_pnl'] = str(f"{int(min_pnl):,}") + str(' [') + str(min_for_accrual) + str('d]') + str(' [') + str(min_pnl_rtn) + str('%]')
                self.trades_df.loc[index_0, 'current_pnl'] = str(f"{int(current_pnl):,}") + str(' [') + str(current_pnl_rtn) + str('%]')+ str(' [profitable:') + str(percentage_of_pnl) + str('%]')

                if current_pnl >= target_return_abs:
                    if current_pnl >= net_carry_to_maturity_abs:
                        self.trades_df.loc[index_0, 'target_reached'] = 'CLOSE ME!'
                    else:
                        self.trades_df.loc[index_0, 'target_reached'] = str('Y') + str(' (') + str("{:,}".format(int(net_carry_to_maturity_abs-current_pnl))) + str(')')
                else:
                    self.trades_df.loc[index_0, 'target_reached'] = ''
                self.trades_df.loc[index_0, 'matured'] = 'Y' if (locals()['index_1_index_maturity_year']<0 or locals()['index_2_index_maturity_year'] < 0) else ''
                self.trades_df.loc[index_0, 'target_return_pnl'] = target_return_abs
                self.trades_df.loc[index_0, 'pnl_to_maturity'] = net_carry_to_maturity_abs
                row_0['Maturity 1'], row_0['Maturity 2'] = str(locals()['index_1_index_maturity_year_then']), str(locals()['index_2_index_maturity_year_then'])
                self.trades_df.loc[index_0, 'pnl_to_maturity_post_defaults'] = default_analysis.calculate_abs_net_after_default_carry(row=row_0)
                # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
                # finding daily cumulative pnl
                daily_cumulative_pnl_pairs_df = trades_analysis.calculate_cumulative_series(final_df=daily_cumulative_pnl_pairs_df,
                                                                                            combined_df=combined_quotes_df,
                                                                                            trade_date=trade_date)
                daily_cumulative_pnl_hybrid_df = trades_analysis.calculate_cumulative_series_hybrid(final_df=daily_cumulative_pnl_hybrid_df,
                                                                                                    combined_df=combined_quotes_df,
                                                                                                    target_return=target_return_abs,
                                                                                                    net_carry_maturity = net_carry_to_maturity_abs,
                                                                                                    trade_date=trade_date)
                daily_cumulative_pnl_close_trades_df = trades_analysis.calculate_cumulative_series_close_trades(final_df=daily_cumulative_pnl_close_trades_df,
                                                                                                                combined_df=combined_quotes_df,
                                                                                                                target_return=target_return_abs,
                                                                                                                trade_date=trade_date)
               # for long risk legs of trades
                #if direction_multiplier_1 == -1:
                #    combined_quotes_df_long = combined_quotes_df[['pricedate','rolling_pnl_2', 'duration_y']]
                #    combined_quotes_df_long.rename(columns={'rolling_pnl_2':'rolling_pnl','duration_y':'duration'}, inplace=True)
                #    long_risk_upfront = index_2_upfront_amount
                #else:
                #    combined_quotes_df_long = combined_quotes_df[['pricedate', 'rolling_pnl_1', 'duration_x']]
                #    combined_quotes_df_long.rename(columns={'rolling_pnl_1': 'rolling_pnl', 'duration_x':'duration'}, inplace=True)
                #    long_risk_upfront = index_1_upfront_amount
                #daily_cumulative_pnl_long_risk_df = trades_analysis.calculate_cumulative_series(daily_cumulative_pnl_long_risk_df, combined_quotes_df_long,long_risk_upfront, trade_date)

                # generate exposures
                    # by index name against duration buckets cr01 terms
                exposures_by_index_name_df = trades_analysis.calculate_exposure_by_index_name(exposures_by_index_name_df,
                                                                                              row=row_0,
                                                                                              index_1_index_maturity_year=locals()['index_1_index_maturity_year'], index_2_index_maturity_year=locals()['index_2_index_maturity_year'],
                                                                                              direction_multiplier_1=locals()['direction_multiplier_1'], direction_multiplier_2=locals()['direction_multiplier_2'],
                                                                                              index_1_upfront_amount=locals()['index_1_upfront_amount'], index_2_upfront_amount=locals()['index_2_upfront_amount'],
                                                                                              index_1_cash_usage = current_cash_usage_1 , index_2_cash_usage=current_cash_usage_2 )
                    # by ticker( net notional and cr01) (###IMPROVE FOR MISSING CDS CONSTITUENTS)
                exposures_by_ticker_df = trades_analysis.calculate_exposure_by_ticker(exposures_by_ticker_df=exposures_by_ticker_df,
                                                                                      current_tranche_index_properties=self.current_tranche_index_properties,
                                                                                      row=row_0,
                                                                                      index_1_index_maturity_year=locals()['index_1_index_maturity_year'], index_2_index_maturity_year=locals()['index_2_index_maturity_year'],
                                                                                      cds_constituents_1=locals()['cds_constituents_1'], cds_constituents_2=locals()['cds_constituents_2'],
                                                                                      direction_multiplier_1=locals()['direction_multiplier_1'], direction_multiplier_2=locals()['direction_multiplier_2'])

                for df in [daily_cumulative_pnl_pairs_df]:
                    df['accrual_days_x'] = 365.25
                    df['rtn_outright'] = df.apply(lambda row: trades_analysis.calculate_pnl_rtn(row=row), axis=1)
                    df.drop(columns=['accrual_days_x'], inplace=True)
            except Exception as e:
                print(row_0)
                print(e)
                continue


        # Final
        # -> exposure by product(index and tranches)
            # adding cr01 (all index and tranche)
        exposures_by_index_name_df_pvt_cr01 = pd.pivot_table(exposures_by_index_name_df,values='cr01', index='index', columns='maturity', aggfunc='sum').reset_index()
        exposures_by_index_name_df_pvt_carry = exposures_by_index_name_df.groupby('index')['carry'].sum().reset_index()
        exposures_by_index_name_df_pvt_upfront = exposures_by_index_name_df.groupby('index')['upfront'].sum().reset_index()
        exposures_by_index_name_df_pvt_cash_usage = exposures_by_index_name_df.groupby('index')['cash usage'].sum().reset_index()
            # adding carry (all index and tranche)
        exposures_by_index_name_df = pd.merge(exposures_by_index_name_df_pvt_cr01, exposures_by_index_name_df_pvt_carry, on='index', how='outer')
            # adding upfront (all index and tranche)
        exposures_by_index_name_df = pd.merge(exposures_by_index_name_df, exposures_by_index_name_df_pvt_upfront,on='index', how='outer')
            # adding cash usage (all index and tranche)
        exposures_by_index_name_df = pd.merge(exposures_by_index_name_df, exposures_by_index_name_df_pvt_cash_usage, on='index', how='outer')
            # adding annualised default losses and index hedge notional amount (all index and tranche)
        exposures_by_index_name_df = pd.merge(exposures_by_index_name_df, default_analysis.calculate_portfolio_expected_defaults_by_index(portfolio_df=self.trades_df, current_tranche_index_properties=self.current_tranche_index_properties).expected_default_loss(grouped_by='index'),on='index',how='outer')
            # adding index hedge amount
        exposures_by_index_name_df = pd.merge(exposures_by_index_name_df,default_analysis.calculate_portfolio_expected_defaults_by_index(portfolio_df=self.trades_df,current_tranche_index_properties=self.current_tranche_index_properties).hedged_notional_size_with_index(grouped_by='index'), on='index', how='outer')

        # -> exposure by single name cds (junior mezz cds included if default losses reach atta-deta
        exposures_by_ticker_df.rename(columns={'cds_constituents': 'bbg_cds_ticker'}, inplace=True)
        exposures_by_ticker_df = pd.merge(exposures_by_ticker_df,bbg_cds_ticker_key, on='bbg_cds_ticker', how='left')


        return self.trades_df, daily_cumulative_pnl_pairs_df, daily_cumulative_pnl_close_trades_df,daily_cumulative_pnl_hybrid_df,exposures_by_index_name_df, exposures_by_ticker_df

