import cds_raw_data
import cds_momentum_signal
import cds_calculator
import results_evaluator
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
import tranche_delta_runs
import cds_raw_data_cache
import live_data
import results_evaluator
import trades_analysis
import default_analysis

import re
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
from dateutil.relativedelta import relativedelta
from scipy import stats
from xbbg import blp
import math


def filter_logic_pct_return_vs_cash_usage(row,numerator_col_name, pct_return_vs_cash_usage):

    # ADD CARRY PNL FROM CASH USAGE(row['Net Cash Usage'] / 10000 * row['Notional 1'])

    if row['Net Cash Usage']>0:
        if ((row[numerator_col_name]/10000) * row['Notional 1'])/(row['Net Cash Usage']/10000 * row['Notional 1']) >= pct_return_vs_cash_usage:
            return True
        else:
            return False
    else:
        return True


def calculate_net_carry_to_maturity(row_file, row, end_date):
    if row_file=='pair_trade_df':
        direction_multiplier_1 = row['Notional 1'] * -1 if row['Trade 1'] == 'Buy Protection' else row['Notional 1']
        duration_1 = float(re.match(r'^[\d.]+', row['Maturity 1']).group())
        direction_multiplier_2 = row['Notional 2'] * -1 if row['Trade 2'] == 'Buy Protection' else row['Notional 2']
        duration_2 = float(re.match(r'^[\d.]+', row['Maturity 2']).group())
        total_carry_maturity = ((direction_multiplier_1 * row['Quote 1'] / 10000) * duration_1) + ((direction_multiplier_2 * row['Quote 2'] / 10000) * duration_2)

    elif row_file=='index_tranche_properties': # in bps
        # assume long risk/ selling protection
        duration = row['index_maturity_years']
        quote = row['bid']
        total_carry_maturity = duration * quote

    else:
        total_carry_maturity = 0

    return total_carry_maturity

class index_tranche_results:
    def __init__(self, results_df, end_date):
        self.results_df = results_df
        self.end_date = end_date

    def filter_results_post(self,net_carry_requirement, target_return, absolute_return, pct_return_vs_cash_usage, abs_carry_to_maturity, hold_to_maturity_pl_safe, abs_carry_maturity_post_default):

        # filter results for carry
        if isinstance(net_carry_requirement, (int,float)):
            self.results_df = self.results_df[self.results_df['Net Carry'] >= net_carry_requirement]
        elif isinstance(net_carry_requirement,str):
            if net_carry_requirement == 'positive':
                self.results_df = self.results_df[self.results_df['Net Carry'] >= 0]
            elif net_carry_requirement == 'negative':
                self.results_df = self.results_df[self.results_df['Net Carry'] < 0]
        else:
            self.results_df = self.results_df

        # filter results for target return percent against cash usage
        self.results_df=self.results_df[self.results_df.apply(lambda row: filter_logic_pct_return_vs_cash_usage(row=row, numerator_col_name='Target_Return',pct_return_vs_cash_usage=pct_return_vs_cash_usage), axis=1)]

        # filter results for target spread pickup
        self.results_df = self.results_df[self.results_df['Target_Return'] >= target_return]

        # filter results for absolute return
        self.results_df = self.results_df[self.results_df['Target_Return']/10000 * self.results_df['Notional 1'] >= absolute_return]

        # clean results - beta ratios
        self.results_df = self.results_df[self.results_df['Beta Ratio'] >= 0.15]

        # filter results - net carry to maturity
        # done in main script -> self.results_df['net_carry_to_maturity_abs'] = self.results_df.apply(lambda row: calculate_net_carry_to_maturity(row=row, end_date=self.end_date), axis=1)
        self.results_df = self.results_df[self.results_df['net_carry_to_maturity_abs'] > abs_carry_to_maturity]
        #self.results_df.drop('net_carry_to_maturity_abs', axis=1, inplace=True)

        # filter results - net pnl carry to maturity
        if hold_to_maturity_pl_safe in ['Y','y','Yes','yes']:
            # done in main script -> self.results_df['net_carry_to_maturity_default_abs'] = self.results_df.apply(lambda row: default_analysis.calculate_abs_net_after_default_carry(row=row), axis=1)
            if abs_carry_maturity_post_default < 0:
                abs_carry_maturity_post_default = 0
            self.results_df = self.results_df[self.results_df['net_carry_to_maturity_default_abs'] > abs_carry_maturity_post_default]
            #self.results_df.drop('net_carry_to_maturity_default_abs', axis=1, inplace=True)

        return self.results_df

    @staticmethod
    def find_index_tranche_sub_level(index_short_name, attachment, detachment):

        sub_label_finder = index_short_name.lower().replace(' ', '_') + '_' + 'index_tranche_sub_label'
        sub_label_df = eval(f'variable_keys_beta.{sub_label_finder}')
        sub_label = sub_label_df[(sub_label_df['attachment'] == float(attachment)) & (sub_label_df['detachment'] == float(detachment))]['sub_level'].values[0]

        index_tranche_ranks = pd.DataFrame({
            'sub_level': ['equity', 'junior mezzanine', 'senior mezzanine', 'super senior', 'index'],
            'rank': [5, 3, 2, 1, 4]
        })
        rank = index_tranche_ranks[index_tranche_ranks['sub_level']==sub_label]['rank'].values[0]

        return sub_label, rank

    def results_signal_evaluator(self):
        '''Index-1', 'Series-1', 'Att-Detach 1', Tenor 1 ,'Maturity 1', 'Trade 1','Quote 1'
            'Index-2', 'Series-2','Att-Detach 2', Tenor 2 ,'Maturity 2', 'Trade 2','Quote 2'
            'Beta Ratio', 'Reason', 'Type', 'Percentile', 'T Cost',
            'Target_Return', 'Net Carry', 'Net 12m R+C', 'Net 12m % Rtn', 'Net Basis
            'Notional 1, 'Notional 2', 'Net Upfront','Net Cash Usage' '''


        results_df = self.results_df
        results_df['Signal'] = 0
        results_df['Note'] = ''


        for index, row in results_df.iterrows():
            signal_score = 0
            note = ''

            ### signals ###
            net_core_signal_value = 5

            index_short_name_1, index_short_name_2 = row['Index-1'], row['Index-2']
            attachment_1, detachment_1 = row['Att-Detach 1'].split('-')
            attachment_1, detachment_1 = float(attachment_1), float(detachment_1)
            attachment_2, detachment_2 = row['Att-Detach 2'].split('-')
            attachment_2, detachment_2 = float(attachment_2), float(detachment_2)
            sub_level_1, sub_level_1_rank = index_tranche_results.find_index_tranche_sub_level(index_short_name_1, attachment_1, detachment_1)
            sub_level_2, sub_level_2_rank = index_tranche_results.find_index_tranche_sub_level(index_short_name_2, attachment_2, detachment_2)
            duration_1 = float(re.match(r'^[\d.]+', row['Maturity 1']).group())
            duration_2 = float(re.match(r'^[\d.]+', row['Maturity 2']).group())
            direction_multiplier_1 = 1 if row['Trade 1'] == 'Sell Protection' else -1
            direction_multiplier_2 = 1 if row['Trade 2'] == 'Sell Protection' else -1
            net_r_c = row['Net 12m R+C']
            net_carry = row['Net Carry'] if row['Net Carry'] !=0 else 1
            net_basis = row['Net Basis']

            ############################################  NEW SIGNALLER #########################################################################
            # everything is off net carry values (5). then the rest are multiples 0-1 of that

            # net carry long term > net basis/r+c
            target_return = row['Target_Return']
            target_return_abs = (row['Target_Return']/10000) * row['Notional 1']
            net_carry_abs = (net_carry/10000) * row['Notional 1']
            # net_carry_to_maturity_abs = (direction_multiplier_1 * row['Quote 1']/10000 * row['Notional 1'] * duration_1) + (direction_multiplier_2 * row['Quote 2']/10000 * row['Notional 2'] * duration_2)
            net_carry_to_maturity_abs = row['net_carry_to_maturity_abs']
            net_carry_default_return_abs = row['net_carry_to_maturity_default_abs']

            # long term#
            # net carry a priority
            if net_carry >= 0:
                #score the carry
                signal_score += net_core_signal_value
                note += str('<net carry>')

                #score the basis
                if net_basis >=1:
                    signal_score += signal_score * net_basis/(net_basis+(10/net_basis))
                    note += str('<net basis>')
                elif (net_basis >=0 and net_basis<1):
                    signal_score += signal_score
                else:
                    #signal_score += -1 * signal_score * (abs(net_basis)/(abs(net_basis)+1)) # Old Formula
                    signal_score += -1 * signal_score * (abs(net_basis) / (abs(net_basis) + (10/abs(net_basis))) )
                #score the rolldown and carry
                if net_r_c >= 0:
                    signal_score += net_core_signal_value * (net_r_c/net_carry)
                    note += str('<net r+c>')
                else:
                    signal_score += net_core_signal_value * (net_r_c/net_carry)

                # subordination rank + (sell protection on senior and buy protection on junior/index)(all indices/tranches)
                    # only works for long term holding when there is net carry
                if ((sub_level_1_rank > sub_level_2_rank) and (row['Trade 1'] == 'Buy Protection')) or ((sub_level_2_rank > sub_level_1_rank) and (row['Trade 2'] == 'Buy Protection')):
                    note += str('<long risk senior vs buy risk junior>')
                    signal_score += net_core_signal_value

                    # buy protection index/equity tranche and sell protection non-equity tranche ->
                    if (sub_level_1 in ['equity','index'] and row['Trade 1'] == 'Buy Protection') or (sub_level_2 in ['equity','index'] and row['Trade 2'] == 'Buy Protection'):
                        note += str('<pair with primary defaults returns>')
                        signal_score += net_core_signal_value * (1 + (target_return + (net_carry * min(duration_1, duration_2)))/target_return)

                # default underlying payout from excess notional (variety in tenors)
                if (sub_level_1 =='index' and sub_level_2 == 'index') and (row['Index-1'] == row['Index-2']) and (row['Series-1'] == row['Series-2']):
                    note += str('<default underlying payout from excess notional. same index series>')
                    signal_score += net_core_signal_value * (1 + (target_return + (net_carry * min(duration_1, duration_2)))/target_return)
            else: # net carry <0
                # score the basis
                if net_basis >= 1:
                    signal_score += signal_score * net_basis/(net_basis+(10/net_basis))
                    note += str('<net basis>')
                elif (net_basis >=0 and net_basis<1):
                    signal_score += signal_score
                else:
                    # signal_score += -1 * signal_score * (abs(net_basis)/(abs(net_basis)+1)) # Old Formula
                    signal_score += -1 * signal_score * (abs(net_basis) / (abs(net_basis) + (10 / abs(net_basis))))
                # score the rolldown and carry
                if net_r_c >= 0:
                    signal_score += net_core_signal_value * (net_r_c / net_carry)
                    note += str('<net r+c>')
                else:
                    signal_score += net_core_signal_value * (net_r_c / net_carry)

                # default underlying payount from excess notional
                if (sub_level_1 =='index' and sub_level_2 == 'index') and (row['Index-1'] == row['Index-2']) and (row['Series-1'] == row['Series-2']):
                    note += str('<default underlying payout from excess notional. same index series>')
                    signal_score += net_core_signal_value * (target_return + (net_carry * min(duration_1, duration_2)))/target_return


            # fast liquid dislocation + same index short name
                # short term trade, don't care about carry, rolldown and net basis
            if (sub_level_1 == 'index' and sub_level_2 =='index') and (variable_keys_beta.cds_index_ig_hy[row['Index-1']] == variable_keys_beta.cds_index_ig_hy[row['Index-2']]):

                if net_carry>=0: # need to reset
                    signal_score += -net_core_signal_value
                if variable_keys_beta.cds_index_ig_hy[row['Index-1']] == 'IG':
                    if row['Target_Return']/max(duration_1, duration_2) > 3:  # can improve so its dynamic
                        note += str('<fast liquid dislocations IG IG>')
                        signal_score += net_core_signal_value * ((net_carry/target_return)+1)
                elif variable_keys_beta.cds_index_ig_hy[row['Index-1']] == 'HY':
                    if row['Target_Return']/max(duration_1, duration_2) > 15:  # can improve so its dynamic
                        note += str('<fast liquid dislocations HY HY>')
                        signal_score += net_core_signal_value * ((net_carry / target_return) + 1)
                else:
                    signal_score += 0


            target_return_abs_decider = 0

            # -> avoid: sell protection index (default chance) vs any -> HY (3.5 defaults) <-
            if ((sub_level_1=='index' or sub_level_1=='equity') and row['Trade 1'] == 'Sell Protection' and variable_keys_beta.cds_index_ig_hy[row['Index-1']]=='HY') or ((sub_level_2=='index' or sub_level_2=='equity') and row['Trade 2'] == 'Sell Protection' and variable_keys_beta.cds_index_ig_hy[row['Index-2']] == 'HY'):
                avoid_index = row['Index-1'] if row['Trade 1'] == 'Sell Protection' else row['Index-2']
                avoid_sub_label = sub_level_1 if row['Trade 1'] == 'Sell Protection' else sub_level_2
                avoid_notional = row['Notional 1'] if row['Trade 1'] == 'Sell Protection' else row['Notional 2']

                if net_carry>=0: # need to reset
                    signal_score += -net_core_signal_value

                if avoid_sub_label =='index':
                    if net_carry_default_return_abs >= 0:
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * net_core_signal_value) + signal_score
                    else:
                        note += str('<warning: -ve return vs default on sell protection HY index>')
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * signal_score)

                else:
                    note += str('<warning: sell protection HY equity>')
                    if net_carry_default_return_abs >= 0:
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * net_core_signal_value) + signal_score
                    else:
                        note += str('<warning: -ve return vs default on sell protection HY equity>')
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * signal_score)

            # -> avoid: sell protection index (default chance) vs any -> IG (1.5 default) <-
            elif ((sub_level_1=='index' or sub_level_1=='equity') and row['Trade 1'] == 'Sell Protection' and variable_keys_beta.cds_index_ig_hy[row['Index-1']]=='IG') or ((sub_level_2=='index' or sub_level_2=='equity') and row['Trade 2'] == 'Sell Protection' and variable_keys_beta.cds_index_ig_hy[row['Index-2']] == 'IG'):
                avoid_index = row['Index-1'] if row['Trade 1'] == 'Sell Protection' else row['Index-2']
                avoid_sub_label = sub_level_1 if row['Trade 1'] == 'Sell Protection' else sub_level_2
                avoid_notional = row['Notional 1'] if row['Trade 1'] == 'Sell Protection' else row['Notional 2']

                if net_carry>=0: # need to reset
                    signal_score += -net_core_signal_value
                if avoid_sub_label == 'index':
                    note += str('<warning: sell protection IG index>')
                    if net_carry_default_return_abs >= 0:
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * net_core_signal_value) + signal_score
                    else:
                        note += str('<warning: -ve carry vs default on sell protection IG index>')
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * signal_score)
                else:
                    note += str('<warning: sell protection IG equity>')
                    if net_carry_default_return_abs >= 0:
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * net_core_signal_value) + signal_score
                    else:
                        note += str('<warning: -ve carry vs default on sell protection IG Equity>')
                        signal_score = (((target_return_abs_decider + net_carry_default_return_abs)/target_return_abs) * signal_score)
            else: #all other pair trades not sell protection index or equity
                signal_score = (((target_return_abs_decider + net_carry_to_maturity_abs)/target_return_abs) * signal_score)

            # evaluating the strength and importance of the target return with the other signals
                # can improve
            evaluated_signal_strength = signal_score

            results_df.loc[index, 'Signal'] = round(evaluated_signal_strength,1)
            results_df.loc[index, 'Note'] = note

        # filter trades with signal > 0. (anything less are bad trades)
        results_df = results_df[results_df['Signal']>0]

        self.results_df = results_df

        return self.results_df


    def clean_results_displayed(self):
        # clean the results
        self.results_df['Percentile'] = self.results_df['Percentile'].round(0)
        self.results_df['Net Carry'] = self.results_df['Net Carry'].round(1)
        self.results_df['Target_Return'] = self.results_df['Target_Return'].round(1)
        self.results_df['T Cost'] = self.results_df['T Cost'].round(2)
        self.results_df['Beta Ratio'] = self.results_df['Beta Ratio'].round(2)
        self.results_df['Net 12m R+C'] = self.results_df['Net 12m R+C'].round(0)
        self.results_df['Net Basis'] = self.results_df['Net Basis'].round(0)

        self.results_df = self.results_df.sort_values(by='Signal', ascending=False)

        return self.results_df