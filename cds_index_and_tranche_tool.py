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
import trades_analysis
import liquidity_analysis
import beta_adjustments
import cds_margin_im_vm
import default_analysis
import results_index_tranche_properties

import pandas as pd
pd.set_option("compute.use_numexpr", False)
pd.set_option("compute.use_bottleneck", False)
from pandas.tseries.offsets import BDay
import numpy as np
import sys
import pickle
import os
import re
sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats
from xbbg import blp
import faulthandler

faulthandler.enable()

def todays_date():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    return today

def last_business_date(date = datetime.datetime.today()):
    date = datetime.datetime.strptime(date, "%Y-%m-%d") if not isinstance(date,datetime.datetime) else date
    last_business_day = (date - BDay(1)).strftime('%Y-%m-%d')
    return last_business_day

def calculate_rtn(pnl_return, cash_usage):
    # calculate % of return
    try:
        if cash_usage < 0:
            if pnl_return > 0:
                pct_return = 100
            else:
                pct_return = round((pnl_return / (cash_usage)) * 100) * -1
        else:
            pct_return = round((pnl_return / (cash_usage)) * 100)
    except:
        pct_return = 0
    return pct_return

class cds_index_tranche_analysis:
    #unpacking parameters
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)

        if not bool(len(pd.bdate_range(self.end_date, self.end_date))):
            new_end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d") if not isinstance(self.end_date, datetime.datetime) else self.end_date
            self.end_date = (new_end_date - BDay(1)).strftime('%Y-%m-%d')

        # Load data and Cache it. All
        #tranche and indices
        self.index_tranche_quotes_df = cds_raw_data_cache.load_data(module=cds_raw_data,function='cds_index_tranche_spread_quotes', directory='historic_raw_data', filename='index_tranche_quotes_df.pickle', start_date=self.start_date, end_date=self.data_end_date, pricing_source=self.pricing_source, actual_end_date=self.end_date, maturity_range=self.maturity_range)

        # tranche and indices constituents no quotes
        self.cds_index_tranche_cds_constituents_all = cds_raw_data_cache.load_data(module=cds_raw_data, function='cds_index_tranche_spread_cds_constituents', directory='historic_raw_data', filename='cds_index_tranche_cds_constituents_all.pickle', end_date=self.data_end_date)

        #single name cds and quotes historically
        # defaulted cds
        self.cds_defaults = cds_raw_data.cds_defaults() #cds_weight=0 -> defaulted cds
        # all cds quote data by date
        self.all_cds_df = cds_raw_data_cache.load_data(module=cds_raw_data, function='cds_quote_data', directory='historic_raw_data', filename='tranche_index_all_cds_df.pickle', start_date=self.start_date, end_date=self.data_end_date)
        # ratings adjustment - takes worst rating for ticker/seniority day by day on all bonds. ref ob might not have been live in certain days in the past when it got set up
        self.all_cds_df = cds_raw_data_cache.load_data(module=cds_raw_data, function='historic_ratings_by_ticker_seniority', directory='historic_raw_data', filename='tranche_index_all_cds_df_adjusted.pickle', start_date=self.start_date, end_date=self.data_end_date, data_frame=self.all_cds_df)

        # incorporate an issuers attributes now vs history relatively
        # checks for ratings changes and adjusts + 2 months rating lag adjustment
        if self.beta_historically_adjusted_attribute_changes in ['Y', 'Yes', 'y', 'yes']:
            self.all_cds_df = cds_raw_data_cache.load_data(module=variable_keys_beta, function='run_processes', directory='historic_raw_data', filename='tranche_index_all_cds_df_adjusted_beta_attributes.pickle', start_date=self.start_date, end_date=self.data_end_date, data_frame=self.all_cds_df, process='beta_adjust_historical_attribute_changes_vs_now')

        # Latest/Live Quotes + Historical EOD Quotes
            # index/tranche = current quotes=live
            # cds = current quotes = T-1 (amend for live)

        # tranche and indices (index first, then tranche after)
        today_live_index_quotes_df = live_data.cds_index_live_data(dataframe=self.index_tranche_quotes_df,end_date=self.end_date )
        today_live_tranche_quotes_df = live_data.cds_tranche_live_data(live_index_dataframe=today_live_index_quotes_df, dataframe=self.index_tranche_quotes_df, end_date=self.end_date)
        self.index_tranche_quotes_df = self.index_tranche_quotes_df[self.index_tranche_quotes_df['pricedate'] != self.end_date] # delete any data that's stored else it will duplicate quotes for dates historically
        self.index_tranche_quotes_df = pd.concat([self.index_tranche_quotes_df, today_live_index_quotes_df,today_live_tranche_quotes_df], ignore_index=True) # combine live+historic quotes

        # single name cds (T-1 at the minute)
            # current_all_cds_df = live_data.cds_live_data(df=all_cds_df, date_range_end=date_range_end) #too many line items since no filter process
        self.cds_current_quotes = cds_raw_data.cds_quote_data(last_business_date(self.end_date), last_business_date(self.end_date),self.pricing_source)
        # back up cds quotes (Uses T-1)
        if self.pricing_source == 'markit':
            self.backup_cds_current_quotes = cds_raw_data.cds_quote_data(last_business_date(self.end_date), last_business_date(self.end_date),'bloomberg')
        else:  # lower(self.pricing_source) == 'bloomberg':
            self.backup_cds_current_quotes = cds_raw_data.cds_quote_data(last_business_date(self.end_date), last_business_date(self.end_date),'markit')
        # Generate full cds spread ranges df (uses t-1)
        self.cds_spread_range_df = cds_raw_data.cds_spread_range_data(end_date=self.end_date, source='bloomberg')
        # apply single name cds momentum and momentum spread
        if self.forward_momentum in ['Y', 'Yes', 'y', 'yes']:
            self.cds_current_quotes = cds_raw_data_cache.load_data(module=None, function=None, process='calculate_cds_momentums', directory='historic_raw_data', filename='current_cds_momentums.pickle', data_frame=self.cds_current_quotes, pricing_source=self.cds_spread_range_df)
        else:
            self.cds_current_quotes['momentum'] = 0
            self.cds_current_quotes['momentum_spread'] = 0

        # load latest interest rates swap curves (t-1)
        self.interest_rates_swap_curves_df = cds_raw_data.interest_rates_swap_curves(end_date=last_business_date(self.end_date))

        # load fx rates by pricedate
        self.fx_rates = cds_raw_data_cache.load_data(module=cds_raw_data, function='fx_rates', directory='historic_raw_data',filename='fx_rates.pickle', start_date=self.start_date, actual_end_date=self.end_date, process='fx_rates')
        todays_live_fx = live_data.fx_live_data(df=self.fx_rates,end_date=self.end_date)
        self.fx_rates = pd.concat([self.fx_rates, todays_live_fx], ignore_index=True)

        # Create Dataframe Keys
        index_tranche_class = cds_raw_data.generate_dataframes()

        # historical data - per pricedate, the unique index, series and version combinations
        self.index_tranche_generic_key_df = index_tranche_class.index_tranche_generic_key_df(df=self.index_tranche_quotes_df)
        # historical data -  per pricedate and index, latest series and latest version
        self.index_tranche_latest_versions_series_generic_key_df = index_tranche_class.index_tranche_latest_versions_series_generic_key_df(df=self.index_tranche_generic_key_df)
        # historical data - per pricedate and index and series, latest versions
        self.index_tranche_latest_versions_generic_key_df = index_tranche_class.index_tranche_latest_versions_generic_key_df(df=self.index_tranche_generic_key_df)

        # Combine Keys + Quotes to create new dataframes
        # historical data -  per pricedate and index, latest series and latest version
        self.index_tranche_latest_versions_series_historic_quotes_df = self.index_tranche_quotes_df.merge(self.index_tranche_latest_versions_series_generic_key_df,on=['pricedate', 'index_short_name', 'index_series', 'index_version'], how='inner')
        # historical data with quotes - per pricedate and index and series, latest versions
        self.index_tranche_latest_versions_historic_quotes_df = self.index_tranche_quotes_df.merge(self.index_tranche_latest_versions_generic_key_df,on=['pricedate', 'index_short_name', 'index_series', 'index_version'], how='inner')

    def index_tranche_rolldown_hedge_basis_analysis(self):

        #  Today's active indices and tranches
        index_tranche_current_quote_df = self.index_tranche_quotes_df[self.index_tranche_quotes_df['pricedate'] == self.end_date]
        index_tranche_latest_versions_df = index_tranche_current_quote_df.merge(self.index_tranche_latest_versions_generic_key_df, on=['pricedate','index_short_name','index_series','index_version'], how='inner')
        index_tranche_latest_versions_df = index_tranche_latest_versions_df[index_tranche_latest_versions_df['index_maturity'] >= self.end_date]

        end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
        rolldown_date, rolldown_date_tag = end_date + relativedelta(months=12) , '12m'

        # clean raw data by correcting the format
        index_tranche_latest_versions_df['spread_quote'] = pd.to_numeric(index_tranche_latest_versions_df['spread_quote'], errors='coerce')

        # filter cds indices and tranches and tenors required for final results
        unfiltered_index_tranche_latest_versions_df = index_tranche_latest_versions_df #unfiltered as raw data has non 5Y tenors for tranches that don't trade but is used for rolldown analysis
        index_tranche_latest_versions_df =index_tranche_latest_versions_df[
                                                ((index_tranche_latest_versions_df['macro_product'] == 'index') & (index_tranche_latest_versions_df['tenor'].isin(self.cds_index_tenor))) |
                                                ((index_tranche_latest_versions_df['macro_product'] == 'tranche') & (index_tranche_latest_versions_df['tenor'].isin(self.cds_tranche_tenor)))
        ]

        # FOR TESTING:
        # index_tranche_latest_versions_df = index_tranche_latest_versions_df[(index_tranche_latest_versions_df['index_short_name'] == 'CDX IG') &
        #                                                                     (index_tranche_latest_versions_df['macro_product'] == 'tranche') &
        #                                                                     (index_tranche_latest_versions_df['index_series'] == 45)]

        # analyse the data row by row
        for index, df_row in index_tranche_latest_versions_df.iterrows():
            row_index = index
            index_short_name = df_row['index_short_name']
            index_series = df_row['index_series']
            index_version = df_row['index_version']
            index_attachment = df_row['attachment']
            index_detachment = df_row['detachment']
            index_maturity_years = df_row['index_maturity_years'] #maturity years as of date end
            index_spread_quote = df_row['spread_quote']
            index_coupon = df_row['index_coupon']
            index_currency = df_row['index_currency']
            index_macro_product = df_row['macro_product']
            index_tenor = df_row['tenor']

            carry_years_date = ((pd.to_datetime(rolldown_date) - pd.to_datetime(end_date)).days) / 365.25
            years_date = index_maturity_years - carry_years_date
            method_type= 'R+C-Same Series' #'R+C-Diff Series'

            # calculate upfront amount
            try:
                upfront_amount = cds_calculator.cds_upfront_calculator(swap_curve_df=self.interest_rates_swap_curves_df,currency=index_currency,maturity_years=index_maturity_years,coupon=index_coupon - index_spread_quote,end_date=end_date)
            except:
                upfront_amount = 1

            # margin requirements (initial margin)
            df_row = cds_margin_im_vm.compute_daily_vm_im_margin_macro(df=df_row,traded_spread=df_row['spread_quote'],b_s_protection='Sell Protection', process='product by product')
            margin_bps = df_row['margin']

            cash_usage_bps = upfront_amount + margin_bps

            # calculate cash usage carry. need to adjust if the cash usage is benchmarked. impacts overall total returns
            cash_usage_carry = cash_usage_bps * (self.cash_benchmarked/10000)
            total_carry_cash_usage_bps = round(cash_usage_bps * index_maturity_years * (self.cash_benchmarked)/10000)

            # calculate and display transaction costs
            index_transaction_cost = transaction_cost_calculator.calculate_transaction_cost_bp(attachment=index_attachment, detachment=index_detachment, product=index_macro_product,index_short_name=index_short_name, tenor=index_maturity_years)
            index_tranche_latest_versions_df.loc[index, 'bid'] = int(index_spread_quote-(index_transaction_cost/2))
            index_tranche_latest_versions_df.loc[index, 'ask'] = int(index_spread_quote+(index_transaction_cost/2))

            # add the liquidity notional
            liquidity_class = liquidity_analysis.index_tranche(index_tranche_latest_versions_df=index_tranche_latest_versions_df, df_row=df_row)
            index_tranche_latest_versions_df.loc[index, 'liquid_notional'] = round(liquidity_class.liquidity_calculator(),0)

            # calculate and display historic spread ranges (same product)
            spread_ranges = spread_ranges_generator.historic_spread_ranges_generator(data_to_run=df_row, data_quotes=self.index_tranche_latest_versions_historic_quotes_df, start_range=(end_date-relativedelta(years=3)).strftime('%Y-%m-%d'), end_range=end_date.strftime('%Y-%m-%d'))
            index_tranche_latest_versions_df.loc[index, 'spread_ranges'] = spread_ranges

            # display upfront amount
            index_tranche_latest_versions_df.loc[index, 'upfront_bps'] = round(upfront_amount)
            # display margin
            index_tranche_latest_versions_df.loc[index, 'margin_bps'] = round(margin_bps)
            # display cash usage
            index_tranche_latest_versions_df.loc[index, 'cash_usage_bps'] = round(cash_usage_bps)

            # --------------------------------------------------------------------------------------------------------------------------------------------------------------------
            # realised loss, basis or hedge analysis by cds constituents
            # --------------------------------------------------------------------------------------------------------------------------------------------------------------------
            cds_index_tranche_vs_cds_analysis_instance = cds_calculator.cds_index_tranche_vs_cds_analysis(all_cds_constituents_df=self.cds_index_tranche_cds_constituents_all, cds_current_quotes_df=self.cds_current_quotes, data_to_run=df_row, backup_cds_current_quotes=self.backup_cds_current_quotes)

            # calculate and display default into [realised loss, default count]
            default_count, default_list, realised_notional_loss = cds_index_tranche_vs_cds_analysis_instance.calculate_realised_loss_and_defaults()

            # clean results for display
            if default_count != 0:
                index_tranche_latest_versions_df.loc[index, 'realised_loss'] = str(round(realised_notional_loss, 3)) + str('[') + str(round(default_count, 0)) + str('] ' + str(default_list))

            # calculate and display basis [ranked single name cds by cds and their recovery vs the attachment/detachment ranges]
            if (index_short_name =='ITRAXX XOVER' and  index_series == 34 and index_attachment == 0.1 and index_detachment == 0.2):
                print('find missing basis list')
            basis, basis_list = cds_index_tranche_vs_cds_analysis_instance.calculate_basis()
            basis_spread = round(index_spread_quote - basis,0) if basis != '' else 0
            basis_list = basis_list if basis_list != [] else ''
            index_tranche_latest_versions_df.loc[index, 'basis'] = basis_spread
            index_tranche_latest_versions_df.loc[index, 'basis_hedges'] = str(basis_list)

            # calculate momentum and momentum spreads (assume within rolldown period)
            if self.forward_momentum in ['Y','Yes','y','yes']:
                momentum, momentum_spread = cds_index_tranche_vs_cds_analysis_instance.calculate_momentum_index_tranche_cds_by_cds(source=self.pricing_source)
            else:
                momentum =0
                momentum_spread=0
            index_tranche_latest_versions_df.loc[index, 'momentum'] = round(float(momentum),1)
            index_tranche_latest_versions_df.loc[index, 'momentum_spread'] = round(float(momentum_spread),1)

            # ROLLDOWN + CARRY + BASIS + MOMENTUM SIGNAL
            # R+C + Basis((R+C spread/spread quote) x Basis) + Momentum (momentum spread x years_date(12m from now duration))
            rolldown_carry_instance = cds_calculator.cds_index_tranche_rolldown_carry(index_tranche_df=unfiltered_index_tranche_latest_versions_df, data_to_run_df = df_row, rolldown_date_data=[rolldown_date, rolldown_date_tag], end_date=end_date, cash_usage_carry=cash_usage_carry, method_type=method_type)
            rolldown_carry, rolldown_carry_output_pct_of_quote, spread_quote_rolldown, years_date = rolldown_carry_instance.rolldown_carry()
            RC_BASIS_MOMENTUM = rolldown_carry + (momentum_spread*years_date) # assume basis rolldowns too hence given shorter tenor
            #RC_BASIS_MOMENTUM = rolldown_carry + (basis_spread*(spread_quote_rolldown/index_spread_quote)) + (momentum_spread*years_date)

            index_tranche_latest_versions_df.loc[index, str(rolldown_date_tag) + str(method_type)] = round(RC_BASIS_MOMENTUM)
            # calculate % of return
            RC_rtn_on_cash = calculate_rtn(RC_BASIS_MOMENTUM, cash_usage_bps)
            index_tranche_latest_versions_df.loc[index, str('RC % Rtn')] = RC_rtn_on_cash

            # ROLLDOWN CARRY BASIS MOMENTUM SIGNALLER
            # x-axis : RC_rtn_on_cash , x>=0.1 or use below pct_return_vs_cash_usage, # y-axis : RC_rtn_to_risk, y>0, # z-axis : signal score
            RC_rtn_to_risk = RC_BASIS_MOMENTUM/index_spread_quote # WORKING ON IT
            if RC_rtn_on_cash >= self.pct_return_vs_cash_usage:
                if (RC_rtn_to_risk>=0 and RC_rtn_to_risk <=1):
                    RC_SIGNAL = (1+np.exp(RC_rtn_on_cash/100)) * (np.exp(RC_rtn_to_risk/2)-1)
                elif RC_rtn_to_risk>1:
                    RC_SIGNAL = (1 + np.exp(RC_rtn_on_cash/100)) * (np.exp(RC_rtn_to_risk/2)-0.65)
                else:
                    RC_SIGNAL = 0
            else:
                RC_SIGNAL = 0

            index_tranche_latest_versions_df.loc[index, str('RC SIGNAL')] = round(RC_SIGNAL,1)
            # net carry cash usage bps
            index_tranche_latest_versions_df.loc[index, 'carry_cash_usage_to_maturity_bps'] = total_carry_cash_usage_bps
            #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # post file creation process
        index_tranche_latest_versions_df['net_carry_to_maturity_bps'] = index_tranche_latest_versions_df.apply(lambda row: results_evaluator.calculate_net_carry_to_maturity(row_file='index_tranche_properties', row=row, end_date=self.end_date), axis=1)
        index_tranche_latest_versions_df['net_carry_to_maturity_bps'] = index_tranche_latest_versions_df['net_carry_to_maturity_bps'] - index_tranche_latest_versions_df['carry_cash_usage_to_maturity_bps']
        index_tranche_latest_versions_df['net_carry_to_maturity % Rtn'] = index_tranche_latest_versions_df.apply(lambda row: calculate_rtn(row['net_carry_to_maturity_bps']/index_maturity_years, row['cash_usage_bps']), axis=1) # annualised

        index_tranche_latest_versions_df['net_carry_to_maturity_default_bps'] = index_tranche_latest_versions_df.apply(lambda row: default_analysis.calculate_abs_net_after_default_carry(row=row, row_file='index_tranche_properties'), axis=1)
        index_tranche_latest_versions_df['net_carry_to_maturity_default_bps'] = index_tranche_latest_versions_df['net_carry_to_maturity_default_bps'] - index_tranche_latest_versions_df['carry_cash_usage_to_maturity_bps']
        index_tranche_latest_versions_df['net_carry_to_maturity_default % Rtn'] = index_tranche_latest_versions_df.apply(lambda row: calculate_rtn(row['net_carry_to_maturity_default_bps']/index_maturity_years, row['cash_usage_bps']), axis=1) # annualised

        # clean results in dataframe
        end_results_class =  results_index_tranche_properties.end_results(df=index_tranche_latest_versions_df)
        index_tranche_latest_versions_df = end_results_class.df

        # Export results in excel form
        if not os.path.exists('results_excel'):
            os.makedirs('results_excel')

        output_path = os.path.join('results_excel', 'index_tranche_properties.xlsx')
        df_full = end_results_class.df
        df_simple = end_results_class.simple_end_results()

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df_simple.to_excel(writer, sheet_name='Simplified', index=False)
            df_full.to_excel(writer, sheet_name='Full_Data', index=False)

        self.index_tranche_latest_versions_df = end_results_class.df

        return index_tranche_latest_versions_df


    def index_tranche_pair_trade_analysis(self):


        ### intialise class ### -> obtain full list of tranche/index properties
        cds_index_tranche_analysis.index_tranche_rolldown_hedge_basis_analysis(self)
        ## filter pre results (equity tranche, recent series as of end_date, maturity ranges)
        filtered_index_tranche_latest_versions_df = cds_raw_data.generate_dataframes.filter_results_pre(df=self.index_tranche_latest_versions_df, equity_tranche_included=self.equity_tranche_included, maturity_range=self.maturity_range, cds_series_inclusion = self.cds_series_inclusion, end_date=self.end_date)

        # apply CDS Indices filter
        if len(self.cds_indices) != 0:
            filtered_index_tranche_latest_versions_df = filtered_index_tranche_latest_versions_df[filtered_index_tranche_latest_versions_df['index_short_name'].isin(self.cds_indices)]

        # secondary relapse period for historical regression
        alternative_lookback_date = (datetime.datetime.strptime(self.end_date, '%Y-%m-%d') - datetime.timedelta(days=6*30)).strftime('%Y-%m-%d')

        index_tranche_list_1, index_tranche_list_2 = filtered_index_tranche_latest_versions_df, filtered_index_tranche_latest_versions_df
        index_tranche_pair_trade_results =[]

        #for testing
        #index_tranche_list_1 = index_tranche_list_1[(index_tranche_list_1['index_short_name']=='CDX IG') & (index_tranche_list_1['macro_product']=='index') & (index_tranche_list_1['tenor']=='5Y') & (index_tranche_list_1['index_series']==39)]
        #index_tranche_list_2 = index_tranche_list_2[(index_tranche_list_2['index_short_name']=='ITRAXX FINS SUB') & (index_tranche_list_2['macro_product']=='index') & (index_tranche_list_2['tenor']=='5Y') & (index_tranche_list_2['index_series']==39)]

        for index, pair_set_1 in index_tranche_list_1.iterrows():
            index_short_name_1 = pair_set_1['index_short_name']
            index_series_1 = pair_set_1['index_series']
            index_version_1 = pair_set_1['index_version']
            index_ig_hy_em_1 = pair_set_1['ig_hy_em']
            index_attachment_1 = pair_set_1['attachment']
            index_detachment_1 = pair_set_1['detachment']
            index_maturity_1 = pair_set_1['index_maturity']
            index_tenor_1 = pair_set_1['tenor']
            index_maturity_years_1 = pair_set_1['index_maturity_years']
            index_product_1 = pair_set_1['macro_product']
            roll_carry_1 = pair_set_1['12mR+C-Same Series']
            upfront_amount_1 = pair_set_1['upfront_bps']
            margin_1 = pair_set_1['margin_bps']
            cash_usage_1 = pair_set_1['cash_usage_bps']
            index_basis_1 = pair_set_1['basis']
            index_1_momentum = pair_set_1['momentum']
            index_1_momentum_spread = pair_set_1['momentum_spread']
            index_liquid_notional_1 = pair_set_1['liquid_notional']
            index_bid_1 = pair_set_1['bid']
            index_ask_1 = pair_set_1['ask']
            index_1_currency = variable_keys_beta.cds_index_currency_key[pair_set_1['index_short_name']]
            index_1_fx_rate = fx_calculator.fx_rate_on_date(ccy=variable_keys_beta.cds_index_currency_key[pair_set_1['index_short_name']], fx_conversion=self.fx_conversion, fx_rates_df=self.fx_rates, date=self.end_date)

            index_duration_1 = index_maturity_years_1 # need to amend for areas when mat years used as duration

            # initialise class -> zscore calculations etc
            p1 = cds_calculator.index_tranche_paired_dataframe_analyser(self.start_date,self.start_date,self.end_date,pair_set_1,latest_version_series_df=self.index_tranche_latest_versions_series_historic_quotes_df, latest_version_df=self.index_tranche_latest_versions_historic_quotes_df, beta_data_filter=self.beta_data_filter)
            p2 = cds_calculator.index_tranche_paired_dataframe_analyser(self.start_date,alternative_lookback_date,self.end_date,pair_set_1,latest_version_series_df=self.index_tranche_latest_versions_series_historic_quotes_df, latest_version_df=self.index_tranche_latest_versions_historic_quotes_df, beta_data_filter=self.beta_data_filter)


            # delete pair_set_1 from index_tranche_list_2
            index_tranche_list_2_to_delete = index_tranche_list_2[(index_tranche_list_2['index_short_name'] == index_short_name_1) & (index_tranche_list_2['index_series'] == index_series_1) & (index_tranche_list_2['attachment'] == index_attachment_1) & (index_tranche_list_2['detachment'] == index_detachment_1) & (index_tranche_list_2['index_maturity'] == index_maturity_1)]
            index_tranche_list_2 = index_tranche_list_2[~index_tranche_list_2.index.isin(index_tranche_list_2_to_delete.index)]

            for index, pair_set_2 in index_tranche_list_2.iterrows():
                index_short_name_2 = pair_set_2['index_short_name']
                index_series_2 = pair_set_2['index_series']
                index_version_2 = pair_set_2['index_version']
                index_ig_hy_em_2 = pair_set_2['ig_hy_em']
                index_attachment_2 = pair_set_2['attachment']
                index_detachment_2 = pair_set_2['detachment']
                index_maturity_2 = pair_set_2['index_maturity']
                index_tenor_2 = pair_set_2['tenor']
                index_maturity_years_2 = pair_set_2['index_maturity_years']
                index_product_2 = pair_set_2['macro_product']
                roll_carry_2 = pair_set_2['12mR+C-Same Series']
                upfront_amount_2 = pair_set_2['upfront_bps']
                margin_2 = pair_set_2['margin_bps']
                cash_usage_2 = pair_set_2['cash_usage_bps']
                index_basis_2 = pair_set_2['basis']
                index_2_momentum = pair_set_2['momentum']
                index_2_momentum_spread = pair_set_2['momentum_spread']
                index_liquid_notional_2 = pair_set_2['liquid_notional']
                index_bid_2 = pair_set_2['bid']
                index_ask_2 = pair_set_2['ask']
                index_2_currency = variable_keys_beta.cds_index_currency_key[pair_set_2['index_short_name']]
                index_2_fx_rate = fx_calculator.fx_rate_on_date(ccy=variable_keys_beta.cds_index_currency_key[pair_set_2['index_short_name']], fx_conversion=self.fx_conversion, fx_rates_df=self.fx_rates, date=self.end_date)

                index_duration_2 = index_maturity_years_2 # need to amend for areas when mat years used as duration

                # find pairs where the duration distance is within threshold parameter -> duration_product_variance
                if abs(index_maturity_years_1-index_maturity_years_2) >= self.duration_product_variance:
                    continue #continue to next possible pair

                # apply duration_weighted priority then -> beta adjustments
                beta_ratio_pair, pair_set_1_historic_quotes, pair_set_2_historic_quotes = p1.beta_historical_regression_function(
                                                                                                pair_set_2,
                                                                                                duration_weighted=self.duration_weighted,
                                                                                                beta_calculation_method=self.beta_calculation_method,
                                                                                                beta_sub_method=self.beta_sub_method)
                beta_ratio_pair_2, pair_set_1_historic_quotes_2, pair_set_2_historic_quotes_2 = p2.beta_historical_regression_function(
                                                                                                pair_set_2,
                                                                                                duration_weighted=self.duration_weighted,
                                                                                                beta_calculation_method=self.beta_calculation_method,
                                                                                                beta_sub_method=self.beta_sub_method)
                # setting ratio pair to 0 for messy, unmatched data. We skip this pair
                if (0<=beta_ratio_pair<0.05 or 0<=beta_ratio_pair_2<0.05):
                    continue #next pair to assess

                # Data Calculations ( z score, avg, differences, percentiles etc)
                p1.combined_output_variables()
                p2.combined_output_variables()

                # Momentum Signal Adjustment
                p1.momentum_adjustor(index_1_momentum, index_1_momentum_spread, index_2_momentum, index_2_momentum_spread, beta_ratio_pair)

                # calculate transaction costs (in bps)
                index_1_transaction_cost = transaction_cost_calculator.calculate_transaction_cost_bp(attachment =index_attachment_1, detachment=index_detachment_1, product=index_product_1, index_short_name=index_short_name_1, tenor=index_maturity_years_1)
                index_2_transaction_cost = transaction_cost_calculator.calculate_transaction_cost_bp(attachment =index_attachment_2, detachment=index_detachment_2, product=index_product_2, index_short_name=index_short_name_2, tenor=index_maturity_years_2)
                    # divided by 2 because we take mids for quotes/snaps as the point to add/subtract the transaction cost from
                index_transaction_cost_bp = ((index_1_transaction_cost/2) + ((index_2_transaction_cost/2) * (1 / beta_ratio_pair)))
                index_transaction_cost_bp_fx = (((index_1_transaction_cost * index_1_fx_rate)/2) + (((index_2_transaction_cost * index_2_fx_rate)/2) * (1 / beta_ratio_pair)))

                # calculate notionals for pair trade
                index_pair_1_notional, index_pair_2_notional = liquidity_analysis.index_tranche.calculate_pair_notionals(index_liquid_notional_1, index_liquid_notional_2, beta_ratio_pair)
                # stupid notionals because the beta is high
                if abs(index_pair_1_notional)<10 or abs(index_pair_2_notional)<10:
                    continue

                # find the most extreme z score
                z_score_current_of_difference = p1.z_score_current_of_difference/abs(p1.z_score_current_of_difference) * (max(abs(p1.z_score_current_of_difference), abs(p2.z_score_current_of_difference)))

                # Final (+fx adjustments and duration adjustments)
                # Ensure both time zones have the same direction for the z direction
                if (p1.z_score_current_of_difference>0 and p2.z_score_current_of_difference>0) or (p1.z_score_current_of_difference<0 and p2.z_score_current_of_difference<0):
                    # index_1 momentum = index_2 momentum
                    if (p1.index_1_momentum == 0) and (p1.index_2_momentum==0):
                        if abs(z_score_current_of_difference) * p1.st_dev_of_difference * min(index_1_fx_rate, index_2_fx_rate) > index_transaction_cost_bp_fx:  # transaction cost filter based for mean reversion logic
                            if z_score_current_of_difference > 0:

                                # net return from cr01 move. Given duration mismatch and fx adjustment
                                direction_1_cr01_move = (index_duration_1 * (p1.z_score_current_x * p1.st_dev_of_x) * index_1_fx_rate)
                                direction_2_cr01_move = (index_duration_2 * ((p1.z_score_current_y * p1.st_dev_of_y)/beta_ratio_pair) * index_2_fx_rate)
                                net_direction_cr01_transaction_cost = ((index_1_transaction_cost/2)* index_duration_1 * index_1_fx_rate) + ((index_2_transaction_cost/(2*beta_ratio_pair))* index_duration_2 * index_2_fx_rate)
                                net_direction_cr01_move = direction_1_cr01_move - direction_2_cr01_move - net_direction_cr01_transaction_cost
                                net_upfront = ((upfront_amount_1 * index_1_fx_rate)- ((upfront_amount_2 * index_2_fx_rate)/beta_ratio_pair))
                                net_cash_usage = ((cash_usage_1 * index_1_fx_rate)- ((cash_usage_2 * index_2_fx_rate)/beta_ratio_pair))
                                net_carry_cash_usage = -1 * ((cash_usage_1/10000 * index_1_fx_rate * index_duration_1 * index_pair_1_notional) - (cash_usage_2/10000 * index_2_fx_rate * index_duration_2 * index_pair_2_notional)) * (self.cash_benchmarked/10000)

                                # pair_sub_direction_matters
                                if self.pair_sub_direction_matters == 'Y':
                                    if net_direction_cr01_move < 0:
                                        continue

                                index_1_bs, index_2_bs, condition_reason_note = 'Sell Protection', 'Buy Protection', 'Historic - Mean Reversion'
                                index_1_quote = index_bid_1 if index_1_bs == 'Sell Protection' else index_ask_1
                                index_2_quote = index_bid_2 if index_1_bs == 'Sell Protection' else index_ask_2
                                comp_decomp_note = 'compression' if p1.current_quote_diff > 0 else 'decompression'
                                net_carry = ((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) - index_transaction_cost_bp_fx
                                #old before fx adjustments    net_carry = p1.current_quote_diff - index_transaction_cost_bp
                                net_carry_roll_down_12m = (roll_carry_1 * index_1_fx_rate) - ((roll_carry_2 * index_2_fx_rate) / beta_ratio_pair)
                                #old before fx adjustments    net_carry_roll_down_12m = roll_carry_1-(roll_carry_2/beta_ratio_pair)

                                try:
                                    if net_cash_usage < 0:
                                        if net_carry_roll_down_12m > 0:
                                            net_carry_roll_down_12m_return = 100
                                        else:
                                            net_carry_roll_down_12m_return = round((net_carry_roll_down_12m / net_cash_usage) * 100) * -1
                                    else:
                                        net_carry_roll_down_12m_return = round((net_carry_roll_down_12m/net_cash_usage)*100)
                                except:
                                    net_carry_roll_down_12m_return = ''

                                try:
                                    net_basis = (index_basis_1 * index_1_fx_rate) - (index_basis_2 * index_2_fx_rate)/beta_ratio_pair
                                except:
                                    net_basis=''


                                index_tranche_pair_trade_results.append(
                                    [index_short_name_1, index_series_1, f"{index_attachment_1}-{index_detachment_1}",index_tenor_1, f"{index_maturity_years_1}({index_maturity_1.strftime('%Y-%m-%d')})", index_1_bs, index_1_quote,
                                     index_short_name_2,index_series_2, f"{index_attachment_2}-{index_detachment_2}",index_tenor_2 ,f"{index_maturity_years_2}({index_maturity_2.strftime('%Y-%m-%d')})", index_2_bs, index_2_quote,
                                     beta_ratio_pair, condition_reason_note, comp_decomp_note, p1.percentile_current_of_difference, index_transaction_cost_bp*2,
                                     net_direction_cr01_move, net_carry, net_carry_roll_down_12m, net_carry_roll_down_12m_return, net_basis, index_pair_1_notional, index_pair_2_notional,round(net_upfront,0),round(net_cash_usage,0), round(net_carry_cash_usage,0)])
                            else:
                                # net return from cr01 move. Given duration mismatch
                                direction_1_cr01_move = (index_duration_1 * (p1.z_score_current_x * p1.st_dev_of_x) * index_1_fx_rate)
                                direction_2_cr01_move = ((index_duration_2) * ((p1.z_score_current_y * p1.st_dev_of_y)/beta_ratio_pair) * index_2_fx_rate)
                                net_direction_cr01_transaction_cost = ((index_1_transaction_cost/2) * index_duration_1 * index_1_fx_rate) + ((index_2_transaction_cost/(2*beta_ratio_pair)) * index_duration_2 * index_2_fx_rate)
                                net_direction_cr01_move = -direction_1_cr01_move + direction_2_cr01_move - net_direction_cr01_transaction_cost
                                net_upfront = ((upfront_amount_1 * -1 * index_1_fx_rate) + ((upfront_amount_2 * index_2_fx_rate) / beta_ratio_pair))
                                net_cash_usage = ((cash_usage_1 * -1 * index_1_fx_rate) + ((cash_usage_2 * index_2_fx_rate) / beta_ratio_pair))
                                net_carry_cash_usage = -1 * ((cash_usage_1/10000 * -1 * index_1_fx_rate * index_duration_1 * index_pair_1_notional) + (cash_usage_2/10000 * index_2_fx_rate * index_duration_2 * index_pair_2_notional)) * (self.cash_benchmarked/10000)


                                # pair_sub_direction_matters
                                if self.pair_sub_direction_matters == 'Y':
                                    if net_direction_cr01_move < 0:
                                        continue

                                index_1_bs, index_2_bs, condition_reason_note = 'Buy Protection', 'Sell Protection', 'Historic - Mean Reversion'
                                index_1_quote = index_bid_1 if index_1_bs =='Sell Protection' else index_ask_1
                                index_2_quote = index_bid_2 if index_1_bs == 'Sell Protection' else index_ask_2
                                comp_decomp_note = 'decompression' if p1.current_quote_diff > 0 else 'compression'
                                net_carry = -1 * (((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) + index_transaction_cost_bp_fx) if ((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) >= 0 else (-1 * (((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate))-index_transaction_cost_bp_fx))
                                net_carry_roll_down_12m = (roll_carry_1 * -1 * index_1_fx_rate) + ((roll_carry_2  * index_2_fx_rate)/ beta_ratio_pair)
                                #net_carry_roll_down_12m_return = (net_carry_roll_down_12m/((upfront_amount_1 * -1 * index_1_fx_rate) + ((upfront_amount_2 * index_2_fx_rate)/beta_ratio_pair)))*100

                                try:
                                    if net_cash_usage < 0:
                                        if net_carry_roll_down_12m > 0:
                                            net_carry_roll_down_12m_return = 100
                                        else:
                                            net_carry_roll_down_12m_return = round((net_carry_roll_down_12m / net_cash_usage) * 100) * -1
                                    else:
                                        net_carry_roll_down_12m_return = round((net_carry_roll_down_12m/net_cash_usage)*100)
                                except:
                                    net_carry_roll_down_12m_return = ''

                                try:
                                    net_basis = (-1 * index_basis_1 * index_1_fx_rate) + (index_basis_2 * index_2_fx_rate)/beta_ratio_pair
                                except:
                                    net_basis = ''

                                index_tranche_pair_trade_results.append(
                                    [index_short_name_1, index_series_1, f"{index_attachment_1}-{index_detachment_1}",index_tenor_1, f"{index_maturity_years_1}({index_maturity_1.strftime('%Y-%m-%d')})", index_1_bs, index_1_quote,
                                     index_short_name_2,index_series_2, f"{index_attachment_2}-{index_detachment_2}",index_tenor_2 ,f"{index_maturity_years_2}({index_maturity_2.strftime('%Y-%m-%d')})", index_2_bs, index_2_quote,
                                     beta_ratio_pair, condition_reason_note, comp_decomp_note, p1.percentile_current_of_difference, index_transaction_cost_bp*2,
                                     net_direction_cr01_move, net_carry, net_carry_roll_down_12m, net_carry_roll_down_12m_return, net_basis, index_pair_1_notional, index_pair_2_notional,round(net_upfront,0),round(net_cash_usage,0), round(net_carry_cash_usage,0)])

                    else: #p1.index_1_momentum, p1.index_2_momentum != 0:
                        if abs((z_score_current_of_difference * p1.st_dev_of_difference * min(index_1_fx_rate, index_2_fx_rate)) + ((p1.index_1_momentum_sign * (p1.index_1_momentum_spread_move * index_1_fx_rate)) - (p1.index_2_momentum_sign * (p1.index_2_momentum_spread_beta_adj * index_2_fx_rate)))) > index_transaction_cost_bp_fx:
                            if z_score_current_of_difference > 0:
                                index_1_bs, index_2_bs, condition_reason_note = 'Sell Protection', 'Buy Protection', 'Momentum Driven'
                                index_1_quote = index_bid_1 if index_1_bs =='Sell Protection' else index_ask_1
                                index_2_quote = index_bid_2 if index_1_bs == 'Sell Protection' else index_ask_2
                                comp_decomp_note = 'compression' if p1.current_quote_diff_momentum > 0 else 'decompression'

                                net_carry = ((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) - index_transaction_cost_bp_fx
                                net_carry_roll_down_12m = (roll_carry_1 * index_1_fx_rate) - ((roll_carry_2 * index_2_fx_rate) / beta_ratio_pair)
                                net_upfront = ((upfront_amount_1 * index_1_fx_rate) - ((upfront_amount_2 * index_2_fx_rate)/beta_ratio_pair))
                                net_cash_usage = ((cash_usage_1 * index_1_fx_rate) - ((cash_usage_2 * index_2_fx_rate)/beta_ratio_pair))
                                net_carry_cash_usage = -1 * ((cash_usage_1/10000 * index_1_fx_rate * index_duration_1 * index_pair_1_notional) - (cash_usage_2/10000 * index_2_fx_rate * index_duration_2 * index_pair_2_notional)) * (self.cash_benchmarked/10000)


                                try:
                                    if net_cash_usage < 0:
                                        if net_carry_roll_down_12m > 0:
                                            net_carry_roll_down_12m_return = 100
                                        else:
                                            net_carry_roll_down_12m_return = round((net_carry_roll_down_12m / net_cash_usage) * 100) * -1
                                    else:
                                        net_carry_roll_down_12m_return = round((net_carry_roll_down_12m/net_cash_usage)*100)
                                except:
                                    net_carry_roll_down_12m_return = ''

                                try:
                                    net_basis = index_basis_1 - index_basis_2/beta_ratio_pair
                                except:
                                    net_basis=''

                                # net return from cr01 move. Given duration mismatch
                                direction_1_cr01_move = (index_duration_1 * (p1.z_score_current_x * p1.st_dev_of_x) * index_1_fx_rate)
                                direction_2_cr01_move = (index_duration_2 * ((p1.z_score_current_y * p1.st_dev_of_y)/beta_ratio_pair) * index_2_fx_rate)
                                net_direction_cr01_transaction_cost = ((index_1_transaction_cost/2)* index_duration_1 * index_1_fx_rate) + ((index_2_transaction_cost/(2*beta_ratio_pair))* index_duration_2 * index_2_fx_rate)
                                net_direction_cr01_move = direction_1_cr01_move - direction_2_cr01_move - net_direction_cr01_transaction_cost

                                # pair_sub_direction_matters
                                if self.pair_sub_direction_matters == 'Y':
                                    if net_direction_cr01_move < 0:
                                        continue

                                index_tranche_pair_trade_results.append(
                                    [index_short_name_1, index_series_1, f"{index_attachment_1}-{index_detachment_1}",index_tenor_1, f"{index_maturity_years_1}({index_maturity_1.strftime('%Y-%m-%d')})", index_1_bs, index_1_quote,
                                     index_short_name_2,index_series_2, f"{index_attachment_2}-{index_detachment_2}",index_tenor_2 ,f"{index_maturity_years_2}({index_maturity_2.strftime('%Y-%m-%d')})", index_2_bs, index_2_quote,
                                     beta_ratio_pair, condition_reason_note, comp_decomp_note, p1.percentile_current_of_difference, index_transaction_cost_bp*2,
                                     net_direction_cr01_move, net_carry, net_carry_roll_down_12m, net_carry_roll_down_12m_return, net_basis, index_pair_1_notional, index_pair_2_notional,round(net_upfront,0),round(net_cash_usage,0), round(net_carry_cash_usage,0)])
                            else:
                                index_1_bs, index_2_bs, condition_reason_note = 'Buy Protection', 'Sell Protection', 'Momentum Driven'
                                index_1_quote = index_bid_1 if index_1_bs == 'Sell Protection' else index_ask_1
                                index_2_quote = index_bid_2 if index_1_bs == 'Sell Protection' else index_ask_2
                                comp_decomp_note = 'decompression' if p1.current_quote_diff_momentum > 0 else 'compression'

                                net_carry = -1 * (((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) + index_transaction_cost_bp_fx) if ((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate)) >= 0 else (-1 * (((p1.index_1_current_quote * index_1_fx_rate) - (p1.index_2_current_quote * index_2_fx_rate))-index_transaction_cost_bp_fx))
                                net_carry_roll_down_12m = (roll_carry_1 * -1 * index_1_fx_rate) + ((roll_carry_2  * index_2_fx_rate)/ beta_ratio_pair)
                                net_upfront = ((upfront_amount_1 * -1 * index_1_fx_rate) + ((upfront_amount_2 * index_2_fx_rate) / beta_ratio_pair))
                                net_cash_usage = ((cash_usage_1 * -1 * index_1_fx_rate) + ((cash_usage_2 * index_2_fx_rate) / beta_ratio_pair))
                                net_carry_cash_usage = -1 * ((cash_usage_1/10000 * -1 * index_1_fx_rate * index_duration_1 * index_pair_1_notional) + (cash_usage_2/10000 * index_2_fx_rate * index_duration_2 * index_pair_2_notional)) * (self.cash_benchmarked/10000)


                                try:
                                    if net_cash_usage < 0:
                                        if net_carry_roll_down_12m > 0:
                                            net_carry_roll_down_12m_return = 100
                                        else:
                                            net_carry_roll_down_12m_return = round((net_carry_roll_down_12m / net_cash_usage) * 100) * -1
                                    else:
                                        net_carry_roll_down_12m_return = round((net_carry_roll_down_12m/net_cash_usage)*100)
                                except:
                                    net_carry_roll_down_12m_return = ''

                                try:
                                    net_basis = -index_basis_1 + index_basis_2/beta_ratio_pair
                                except:
                                    net_basis = ''

                                # net return from cr01 move. Given duration mismatch. (in cts form)
                                direction_1_cr01_move = (index_duration_1 * (p1.z_score_current_x * p1.st_dev_of_x) * index_1_fx_rate)
                                direction_2_cr01_move = ((index_duration_2) * ((p1.z_score_current_y * p1.st_dev_of_y)/beta_ratio_pair) * index_2_fx_rate)
                                net_direction_cr01_transaction_cost = ((index_1_transaction_cost/2) * index_duration_1 * index_1_fx_rate) + ((index_2_transaction_cost/(2*beta_ratio_pair)) * index_duration_2 * index_2_fx_rate)
                                net_direction_cr01_move = -direction_1_cr01_move + direction_2_cr01_move - net_direction_cr01_transaction_cost

                                # pair_sub_direction_matters
                                if self.pair_sub_direction_matters == 'Y':
                                    if net_direction_cr01_move < 0:
                                        continue

                                index_tranche_pair_trade_results.append(
                                    [index_short_name_1, index_series_1, f"{index_attachment_1}-{index_detachment_1}",index_tenor_1, f"{index_maturity_years_1}({index_maturity_1.strftime('%Y-%m-%d')})", index_1_bs, index_1_quote,
                                     index_short_name_2,index_series_2, f"{index_attachment_2}-{index_detachment_2}",index_tenor_2 ,f"{index_maturity_years_2}({index_maturity_2.strftime('%Y-%m-%d')})", index_2_bs, index_2_quote,
                                     beta_ratio_pair, condition_reason_note, comp_decomp_note, p1.percentile_current_of_difference, index_transaction_cost_bp*2,
                                     net_direction_cr01_move, net_carry, net_carry_roll_down_12m, net_carry_roll_down_12m_return, net_basis, index_pair_1_notional, index_pair_2_notional,round(net_upfront,0),round(net_cash_usage,0), round(net_carry_cash_usage,0)])
                else:
                    continue


        results_df = pd.DataFrame(list(index_tranche_pair_trade_results),
                                  columns=['Index-1', 'Series-1', 'Att-Detach 1', 'Tenor 1', 'Maturity 1', 'Trade 1','Quote 1',
                                           'Index-2', 'Series-2', 'Att-Detach 2', 'Tenor 2', 'Maturity 2', 'Trade 2','Quote 2',
                                           'Beta Ratio', 'Reason', 'Type', 'Percentile', 'T Cost',
                                           'Target_Return', 'Net Carry', 'Net 12m R+C', 'Net 12m % Rtn', 'Net Basis', 'Notional 1', 'Notional 2','Net Upfront','Net Cash Usage','net_carry_cash_usage'])

        # add abs carry to maturity (no defaults)
        results_df['net_carry_to_maturity_abs'] = results_df.apply(lambda row: results_evaluator.calculate_net_carry_to_maturity(row_file = 'pair_trade_df', row=row, end_date=self.end_date), axis=1)
        results_df['net_carry_to_maturity_abs'] = results_df['net_carry_to_maturity_abs'] + results_df['net_carry_cash_usage']
        # add abs carry to maturity post expected defaults
        results_df['net_carry_to_maturity_default_abs'] = results_df.apply(lambda row: default_analysis.calculate_abs_net_after_default_carry(row=row), axis=1)
        results_df['net_carry_to_maturity_default_abs'] = results_df['net_carry_to_maturity_default_abs'] + results_df['net_carry_cash_usage']


        # process the results
        if not results_df.empty:
            # initialise
            results = results_evaluator.index_tranche_results(results_df, self.end_date)
            # filter results
            results.filter_results_post(net_carry_requirement=self.net_carry_requirement, target_return=self.target_return,
                                        absolute_return=self.absolute_return, pct_return_vs_cash_usage=self.pct_return_vs_cash_usage,
                                        abs_carry_to_maturity=self.abs_carry_to_maturity, hold_to_maturity_pl_safe=self.hold_to_maturity_pl_safe,
                                        abs_carry_maturity_post_default=self.abs_carry_maturity_post_default)
            # evaluate results
            results.results_signal_evaluator()
            # clean results display
            results_df = results.clean_results_displayed()

        # export results in excel form
        if not os.path.exists('results_excel'):
            os.makedirs('results_excel')
        results_df.to_excel(os.path.join('results_excel', 'index_tranche_pair_trades.xlsx'), index=False, sheet_name='index_tranche_pair_trades')
        return results_df

    def my_portfolio(self):
        my_portfolio_folder_path = os.path.join(os.getcwd(), 'results_excel', 'my_portfolio')
        file_name = 'tranche_portfolio.xlsx'
        my_portfolio_file_path = os.path.join(my_portfolio_folder_path, file_name)

        trades_df = pd.read_excel(my_portfolio_file_path, sheetname='index_tranche_pair_trades', header=0, usecols='A:V')

        # initialise the class for back testing/PnL Analysis
        run_portfolio = trades_analysis.trades_analysis(trades_df=trades_df,
                                                        current_tranche_index_properties = self.index_tranche_latest_versions_df,
                                                        historic_quotes=self.index_tranche_latest_versions_historic_quotes_df,
                                                        interest_rates_swap_curves_df=self.interest_rates_swap_curves_df,
                                                        fx_rates_df = self.fx_rates,
                                                        end_date=self.end_date,
                                                        fx_conversion=self.fx_conversion,
                                                        cds_constituents_df=self.cds_index_tranche_cds_constituents_all,
                                                        cash_benchmarked=self.cash_benchmarked,
                                                        file_path=my_portfolio_file_path)

        output_results, daily_cumulative_pnl_pairs, daily_cumulative_pnl_close_pairs,daily_cumulative_pnl_hybrid_df,exposures_by_index_name_df, exposures_by_ticker_df = run_portfolio.produce_dataframe_and_calculate_pnl

        # produce results in excel format
        # delete the existing Excel file if it exists
        if os.path.exists(my_portfolio_folder_path):
            os.remove(my_portfolio_folder_path)
        with pd.ExcelWriter(my_portfolio_folder_path, engine='openpyxl') as writer:
            # write each DataFrame to a different sheet
            output_results.to_excel(writer, sheet_name='index_tranche_pair_trades', index=False)
            exposures_by_index_name_df.to_excel(writer, sheet_name='exposure_by_index', index=False)
            exposures_by_ticker_df.to_excel(writer, sheet_name='exposure_by_ticker', index=False)
            daily_cumulative_pnl_pairs.to_excel(writer, sheet_name='daily_pnl_pairs', index=False)
            daily_cumulative_pnl_close_pairs.to_excel(writer, sheet_name='daily_pnl_close_pairs', index=False)
            daily_cumulative_pnl_hybrid_df.to_excel(writer, sheet_name='daily_pnl_hybrid', index=False)
        writer.close()


    def back_test(self):


        # LOOPING CODE
        '''# looping through files
        back_test_folder_path = os.path.join(os.getcwd(),'results_excel', 'back_test','file_sets')
        for file_name in os.listdir(back_test_folder_path):
            back_test_file_path = os.path.join(back_test_folder_path, file_name)
            if os.path.isfile(back_test_file_path):

                ###################################### FOR AUTO FINDING PAIR TRADES BASED ON SIGNALs ##########################################################
                auto_on = 'N'
                #back_test_dates = ['2021-02-15', '2021-07-20', '2021-12-15', '2022-03-16', '2022-05-22', '2022-07-07','2022-12-22', '2023-02-23', '2023-05-03', '2023-08-15', '2023-12-05', '2024-01-15','2024-04-16', '2024-06-25', '2024-08-08', '2024-09-27', '2024-11-21', '2025-02-19','2025-04-07']
                back_test_dates = ['2024-02-02', '2024-03-22', '2024-04-19', '2024-06-04','2024-06-26','2024-07-23','2024-08-07','2024-09-26','2024-11-27','2024-12-11', '2025-01-31','2025-02-26','2025-03-28', '2024-04-04','2025-04-10']

                if (len(back_test_dates) != 0 and auto_on == 'Y'):
                    trades_df = pd.DataFrame()
                    file_name = 'back_test_index_tranche_automated.xlsx'
                    back_test_file_path = os.path.join(back_test_folder_path, file_name)
                    trades_to_excel = 'True'
                    for date in back_test_dates:
                        if not bool(len(pd.bdate_range(date, date))):
                            new_end_date = datetime.datetime.strptime(date, "%Y-%m-%d") if not isinstance(date, datetime.datetime) else date
                            date = (new_end_date - BDay(1)).strftime('%Y-%m-%d')

                        index_tranche_tool_back_test = cds_index_tranche_analysis(start_date='2021-01-01',
                                                                                  data_end_date=todays_date(),
                                                                                  end_date=date,
                                                                                  fx_conversion = 'EUR', # different products are in different currencies
                                                                                  pricing_source='markit', cds_index_product_type=['Indices', 'Tranches'],
                                                                                  net_carry_requirement=-25,  #input options ['positive', 'negative', 'all'] or number
                                                                                  target_return=10,
                                                                                  absolute_return=50000,  #
                                                                                  pct_return_vs_cash_usage=10,# percentage return from target return absolute against the cash usage
                                                                                  hold_to_maturity_pl_safe = 'Y',  #'N','Y' means if you dont close the trades let the products roll off. doesnt need to reach target
                                                                                  abs_carry_maturity_post_default=100000,
                                                                                  cash_benchmarked=400, beta_historically_adjusted_attribute_changes='N',
                                                                                  duration_weighted='No',
                                                                                  # run by cds by cds to see where the index/tranche should trade
                                                                                  forward_momentum='N',
                                                                                  duration_product_variance=1.25,
                                                                                  maturity_range=[3.3, 7.2],
                                                                                  equity_tranche_included = 'N',
                                                                                  cds_indices = ['CDX IG','CDX HY','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'],  #input options ['CDX IG','CDX HY','CDX EM','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'], 'ITRAXX MAIN', 'ITRAXX XOVER', 'ITRAXX FINS SNR', 'ITRAXX FINS SUB'
                                                                                  cds_series_inclusion = -6,
                                                                                  beta_calculation_method='historic', beta_sub_method='rolling_tenor_to_exact_to_exact', beta_data_filter = 'Y', pair_sub_direction_matters='Y', cds_index_tenor=['3Y', '5Y', '7Y', '10Y']
                                                                                  )

                        properties_df = index_tranche_tool_back_test.index_tranche_rolldown_hedge_basis_analysis()
                        pair_trades_df = index_tranche_tool_back_test.index_tranche_pair_trade_analysis()

                        if not pair_trades_df.empty:

                            pair_trades_df = pair_trades_df.drop_duplicates(subset=['Index-1', 'Series-1','Att-Detach 1', 'Tenor 1', 'Trade 1','Index-2', 'Series-2', 'Att-Detach 2','Tenor 2','Trade 2'])
                            pair_trades_df = pair_trades_df.sort_values(by='Signal', ascending=False)
                            pair_trades_df = pair_trades_df.head(10).reset_index(drop=True) #how many trades to take
                            pair_trades_df['Trade Date'] = date
                            pair_trades_df = pair_trades_df[['Trade Date','Index-1', 'Series-1','Att-Detach 1', 'Tenor 1', 'Trade 1', 'Index-2', 'Series-2', 'Att-Detach 2','Tenor 2','Trade 2','Type','Beta Ratio','Net Carry','Target_Return','Signal','Notional 1', 'Notional 2','Net Upfront']]
                            pair_trades_df['Quote 1']=0
                            pair_trades_df['Quote 2']=0
                            #find bid ask quotes
                            for i in range(len(pair_trades_df)):
                                attachment_1, detachment_1 = pair_trades_df.loc[i,'Att-Detach 1'].split('-')
                                attachment_1, detachment_1 = float(attachment_1), float(detachment_1)
                                attachment_2, detachment_2 = pair_trades_df.loc[i,'Att-Detach 2'].split('-')
                                attachment_2, detachment_2 = float(attachment_2), float(detachment_2)
                                bid_ask_tag_1 = 'bid' if pair_trades_df.loc[i, 'Trade 1'] == 'Sell Protection' else 'ask'
                                bid_ask_tag_2 = 'bid' if pair_trades_df.loc[i, 'Trade 2'] == 'Sell Protection' else 'ask'

                                pair_trades_df.loc[i,'Quote 1'] = properties_df[(properties_df['index_short_name'] == pair_trades_df.loc[i,'Index-1']) & (properties_df['index_series'] == pair_trades_df.loc[i,'Series-1']) & (properties_df['attachment'] == attachment_1) & (properties_df['detachment'] == detachment_1) & (properties_df['tenor'] == pair_trades_df.loc[i,'Tenor 1'])][bid_ask_tag_1].values[0]
                                pair_trades_df.loc[i,'Quote 2'] = properties_df[(properties_df['index_short_name'] == pair_trades_df.loc[i,'Index-2']) & (properties_df['index_series'] == pair_trades_df.loc[i,'Series-2']) & (properties_df['attachment'] == attachment_2) & (properties_df['detachment'] == detachment_2) & (properties_df['tenor'] == pair_trades_df.loc[i,'Tenor 2'])][bid_ask_tag_2].values[0]

                            trades_df = pd.concat([trades_df, pair_trades_df], ignore_index=True)
                    # format
                    trades_df['Notional 1'] = trades_df['Notional 1'].round(0)
                    trades_df['Notional 2'] = trades_df['Notional 2'].round(0)
                    trades_df['Trade Date'] = pd.to_datetime(trades_df['Trade Date'], format='%Y-%m-%d')
                    trades_df['Ctpy'] = ''

                    #export trades to excel
                    if trades_to_excel == 'True':
                        trades_df.to_excel(back_test_file_path, index=False, sheet_name='index_tranche_pair_trades')
                else:
                    trades_df = pd.read_excel(back_test_file_path, sheet_name='index_tranche_pair_trades', usecols='A:V', header=0)

                # Not accurate for EM in PnL calculations. Don't know exact CDS weights in current set up
                    # cds constituents table has the cds weight as 0 / can assume equal weighting for cdx ig, cdx hy, and all itraxx products

                bt = trades_analysis.trades_analysis(trades_df=trades_df,
                                             current_tranche_index_properties=self.index_tranche_latest_versions_df,
                                             historic_quotes=self.index_tranche_latest_versions_historic_quotes_df,
                                             interest_rates_swap_curves_df=self.interest_rates_swap_curves_df,
                                             fx_rates_df = self.fx_rates,
                                             end_date=self.end_date,
                                             fx_conversion = self.fx_conversion,
                                             cds_constituents_df=self.cds_index_tranche_cds_constituents_all,
                                             cash_benchmarked=self.cash_benchmarked,
                                             file_path=back_test_file_path)

                output_results, daily_cumulative_pnl_pairs, daily_cumulative_pnl_close_pairs,daily_cumulative_pnl_hybrid_df,exposures_by_index_name_df, exposures_by_ticker_df = bt.produce_dataframe_and_calculate_pnl()

                # produce results in excel format
                #old -> trades_df.to_excel(back_test_file_path, index=False, sheet_name='index_tranche_pair_trades')
                # delete the existing Excel file if it exists
                if os.path.exists(back_test_file_path):
                    os.remove(back_test_file_path)
                with pd.ExcelWriter(back_test_file_path, engine='openpyxl') as writer:
                    # write each DataFrame to a different sheet
                    output_results.to_excel(writer, sheet_name='index_tranche_pair_trades', index=False)
                    exposures_by_index_name_df.to_excel(writer, sheet_name='exposure_by_index', index=False)
                    exposures_by_ticker_df.to_excel(writer, sheet_name='exposure_by_ticker', index=False)
                    daily_cumulative_pnl_pairs.to_excel(writer, sheet_name='daily_pnl_pairs', index=False)
                    daily_cumulative_pnl_close_pairs.to_excel(writer, sheet_name='daily_pnl_close_pairs',index=False)
                    daily_cumulative_pnl_hybrid_df.to_excel(writer, sheet_name='daily_pnl_hybrid', index=False)
                    '''



        # ONE FILE CODE
        back_test_folder_path = os.path.join(os.getcwd(),'results_excel', 'back_test')
        file_name = 'TEST PARAMETERS, Best Possible Trade Situations.xlsx'

        back_test_file_path = os.path.join(back_test_folder_path, file_name)

        ###################################### FOR AUTO FINDING PAIR TRADES BASED ON SIGNALs ##########################################################
        auto_on = 'N'
        # #2021-2025
        # back_test_dates = ['2021-02-15', '2021-07-20', '2021-12-15', '2022-03-16', '2022-05-22', '2022-07-07','2022-12-22', '2023-02-23', '2023-05-03', '2023-08-15', '2023-12-05', '2024-01-15','2024-04-16', '2024-06-25', '2024-08-08', '2024-09-27', '2024-11-21', '2025-02-19','2025-04-07']
        # #2025
        # back_test_dates = ['2024-02-02', '2024-03-22', '2024-04-19', '2024-06-04','2024-06-26','2024-07-23','2024-08-07','2024-09-26','2024-11-27','2024-12-11', '2025-01-31','2025-02-26','2025-03-28', '2024-04-04','2025-04-10']
        # # WORST POSSIBLE -> tightest main moments in 2025
        # back_test_dates = ['2025-02-18', '2025-06-11', '2025-07-29','2025-08-18','2025-09-15']
        # # BEST POSSIBLE -> widest/peak main moments 2025
        back_test_dates = ['2025-01-10', '2025-03-10', '2025-03-21','2025-04-08','2025-04-14','2025-05-22','2025-06-19','2025-08-01','2025-09-01','2025-09-25','2025-10-10']

        if (len(back_test_dates) != 0 and auto_on == 'Y'):
            trades_df = pd.DataFrame()
            #file_name = 'back_test_index_tranche_automated.xlsx'
            back_test_file_path = os.path.join(back_test_folder_path, file_name)
            trades_to_excel = 'True'
            for date in back_test_dates:
                if not bool(len(pd.bdate_range(date, date))):
                    new_end_date = datetime.datetime.strptime(date, "%Y-%m-%d") if not isinstance(date, datetime.datetime) else date
                    date = (new_end_date - BDay(1)).strftime('%Y-%m-%d')

                index_tranche_tool_back_test = cds_index_tranche_analysis(start_date='2021-01-01',
                                                                          data_end_date=todays_date(),
                                                                          end_date=date,
                                                                          fx_conversion='EUR',# different products are in different currencies
                                                                          pricing_source='markit', cds_index_product_type=['Indices','Tranches'], #NOT SET UP
                                                                          net_carry_requirement=-15,  #input options ['positive', 'negative', 'all'] or number
                                                                          target_return=10, #in cts to notional 1
                                                                          absolute_return=50000, #
                                                                          pct_return_vs_cash_usage=10, #percentage return from target return absolute against the cash usage
                                                                          hold_to_maturity_pl_safe = 'Y', abs_carry_to_maturity=-50000, abs_carry_maturity_post_default=100000, #'N','Y' means if you dont close the trades let the products roll off. doesnt need to reach target
                                                                          cash_benchmarked=400, beta_historically_adjusted_attribute_changes='N',
                                                                          duration_weighted='No',
                                                                          # run by cds by cds to see where the index/tranche should trade
                                                                          forward_momentum='N',
                                                                          duration_product_variance=1.25,
                                                                          maturity_range=[3.3, 7.2],
                                                                          equity_tranche_included = 'N',
                                                                          cds_indices = ['CDX IG','CDX HY','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'],  #input options ['CDX IG','CDX HY','CDX EM','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'], 'ITRAXX MAIN', 'ITRAXX XOVER', 'ITRAXX FINS SNR', 'ITRAXX FINS SUB'
                                                                          cds_series_inclusion = -3,
                                                                          cds_tranche_tenor=['5Y'],
                                                                          # input options ['3Y','5Y','7Y','10Y']
                                                                          cds_index_tenor=['3Y', '5Y', '7Y', '10Y'],
                                                                          # input options ['3Y','5Y','7Y','10Y']

                                                                          beta_calculation_method='historic', beta_sub_method='rolling_tenor_to_exact_to_exact', beta_data_filter = 'Y', pair_sub_direction_matters='Y'
                                                                          )

                properties_df = index_tranche_tool_back_test.index_tranche_rolldown_hedge_basis_analysis()
                pair_trades_df = index_tranche_tool_back_test.index_tranche_pair_trade_analysis()

                if not pair_trades_df.empty:

                    pair_trades_df = pair_trades_df.drop_duplicates(subset=['Index-1', 'Series-1','Att-Detach 1', 'Tenor 1', 'Trade 1','Index-2', 'Series-2', 'Att-Detach 2','Tenor 2','Trade 2'])
                    pair_trades_df = pair_trades_df.sort_values(by='Signal', ascending=False)
                    pair_trades_df = pair_trades_df.head(3).reset_index(drop=True) #how many trades to take
                    pair_trades_df['Trade Date'] = date
                    pair_trades_df = pair_trades_df[['Trade Date','Index-1', 'Series-1','Att-Detach 1', 'Tenor 1', 'Trade 1', 'Index-2', 'Series-2', 'Att-Detach 2','Tenor 2','Trade 2','Type','Beta Ratio','Net Carry','Target_Return','Signal','Notional 1', 'Notional 2','Net Upfront']]
                    pair_trades_df['Quote 1']=0
                    pair_trades_df['Quote 2']=0
                    #find bid ask quotes
                    for i in range(len(pair_trades_df)):
                        attachment_1, detachment_1 = pair_trades_df.loc[i,'Att-Detach 1'].split('-')
                        attachment_1, detachment_1 = float(attachment_1), float(detachment_1)
                        attachment_2, detachment_2 = pair_trades_df.loc[i,'Att-Detach 2'].split('-')
                        attachment_2, detachment_2 = float(attachment_2), float(detachment_2)
                        bid_ask_tag_1 = 'bid' if pair_trades_df.loc[i, 'Trade 1'] == 'Sell Protection' else 'ask'
                        bid_ask_tag_2 = 'bid' if pair_trades_df.loc[i, 'Trade 2'] == 'Sell Protection' else 'ask'

                        pair_trades_df.loc[i,'Quote 1'] = properties_df[(properties_df['index_short_name'] == pair_trades_df.loc[i,'Index-1']) & (properties_df['index_series'] == pair_trades_df.loc[i,'Series-1']) & (properties_df['attachment'] == attachment_1) & (properties_df['detachment'] == detachment_1) & (properties_df['tenor'] == pair_trades_df.loc[i,'Tenor 1'])][bid_ask_tag_1].values[0]
                        pair_trades_df.loc[i,'Quote 2'] = properties_df[(properties_df['index_short_name'] == pair_trades_df.loc[i,'Index-2']) & (properties_df['index_series'] == pair_trades_df.loc[i,'Series-2']) & (properties_df['attachment'] == attachment_2) & (properties_df['detachment'] == detachment_2) & (properties_df['tenor'] == pair_trades_df.loc[i,'Tenor 2'])][bid_ask_tag_2].values[0]

                    trades_df = pd.concat([trades_df, pair_trades_df], ignore_index=True)
            # format
            trades_df['Notional 1'] = trades_df['Notional 1'].round(0)
            trades_df['Notional 2'] = trades_df['Notional 2'].round(0)
            trades_df['Trade Date'] = pd.to_datetime(trades_df['Trade Date'], format='%Y-%m-%d')
            trades_df['Ctpy'] = ''

            #export trades to excel
            if trades_to_excel == 'True':
                trades_df.to_excel(back_test_file_path, index=False, sheet_name='index_tranche_pair_trades')
        else:
            trades_df = pd.read_excel(back_test_file_path, sheet_name='index_tranche_pair_trades', usecols='A:V', header=0)

        # at the minute it won't work for EM properly when calculating rolling pnl. As we don't know the original weight of the cds at inception
            # cds constituents table has the cds weight as 0
            # can assume equal weighting for cdx ig, cdx hy, and all itraxx products

        # initialise the class for back testing/PnL Analysis
        bt = trades_analysis.trades_analysis(trades_df=trades_df,
                                             current_tranche_index_properties=self.index_tranche_latest_versions_df,
                                             historic_quotes=self.index_tranche_latest_versions_historic_quotes_df,
                                             interest_rates_swap_curves_df=self.interest_rates_swap_curves_df,
                                             fx_rates_df=self.fx_rates,
                                             end_date=self.end_date,
                                             fx_conversion=self.fx_conversion,
                                             cds_constituents_df=self.cds_index_tranche_cds_constituents_all,
                                             cash_benchmarked=self.cash_benchmarked,
                                             file_path=back_test_file_path)

        output_results, daily_cumulative_pnl_pairs, daily_cumulative_pnl_close_pairs,daily_cumulative_pnl_hybrid_df,exposures_by_index_name_df, exposures_by_ticker_df = bt.produce_dataframe_and_calculate_pnl()

        # produce results in excel format
        # delete the existing Excel file if it exists
        if os.path.exists(back_test_file_path):
            os.remove(back_test_file_path)
        with pd.ExcelWriter(back_test_file_path, engine='openpyxl') as writer:
            # write each DataFrame to a different sheet
            output_results.to_excel(writer, sheet_name='index_tranche_pair_trades', index=False)
            exposures_by_index_name_df.to_excel(writer, sheet_name='exposure_by_index', index=False)
            exposures_by_ticker_df.to_excel(writer, sheet_name='exposure_by_ticker', index=False)
            daily_cumulative_pnl_pairs.to_excel(writer, sheet_name='daily_pnl_pairs', index=False)
            daily_cumulative_pnl_close_pairs.to_excel(writer, sheet_name='daily_pnl_close_pairs', index=False)
            daily_cumulative_pnl_hybrid_df.to_excel(writer, sheet_name='daily_pnl_hybrid', index=False)
            




if __name__ == '__main__':

    index_tranche_tool = cds_index_tranche_analysis(start_date='2021-01-01',
                                                    data_end_date = todays_date(),#todays_date()
                                                    end_date= todays_date(), #todays_date()
                                                    fx_conversion = 'EUR', # different products are in different currencies, 'Local' for how it is
                                                    pricing_source='markit',  #input options ['markit','bloomberg'] # to build in likely
                                                    net_carry_requirement=-15,  #input options ['positive', 'negative', 'all']. Impacted by abs carry to maturity
                                                    target_return=10, #in cts to notional 1
                                                    absolute_return=50000, #cash return outright based off of target returns and notionals
                                                    pct_return_vs_cash_usage=10, #percentage return from target return absolute against the cash usage
                                                    hold_to_maturity_pl_safe='Y', abs_carry_to_maturity=-100000, #####>-50k#####
                                                    abs_carry_maturity_post_default=50000,####>-100k!!!!####, abs carry to maturity impact with or without defaults
                                                    cash_benchmarked=0,  # input options 'Yes', 'No' -> SET TO 400BPS IN SCRIPT
                                                    beta_historically_adjusted_attribute_changes='N',
                                                    # input options are 'Y', else everything else is no. Adjust historical beta of single name cds

                                                    # PRIORITY over BETA #
                                                    duration_weighted='No',
                                                    # input options ,'Yes','No'
                                                    duration_product_variance = 1.25,
                                                    # input options - in years
                                                    maturity_range=[3.3, 7],  #lower and upper range. inclusive of -> SET TO -30, 30 TO INCLUDE ALL FOR BACKTESTING

                                                    # Note, Superceded by duration_weighted
                                                    beta_calculation_method='historic',
                                                    #input options - 'historic' or 'cds_by_cds'
                                                    beta_sub_method='rolling_tenor_to_exact_to_exact',
                                                    #input options - 'rolling_tenor', 'exact_to_exact'
                                                    beta_data_filter = 'Y',
                                                    #input options - Y, N

                                                    pair_sub_direction_matters='Y',
                                                    #input options = 'Y', 'N', important for duration and fx adjustments
                                                    equity_tranche_included = 'N',
                                                    #input options = 'Y' , 'N'

                                                    #run by cds by cds to see where the index/tranche should trade
                                                    forward_momentum='N',
                                                    # input options = 'Y', 'N'

                                                    cds_indices=['CDX IG','CDX HY','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'],
                                                    #input options ['CDX IG','CDX HY','CDX EM','ITRAXX MAIN','ITRAXX XOVER','ITRAXX FINS SEN','ITRAXX FINS SUB'], -> EMPTY INCLUDES ALL
                                                    cds_series_inclusion= -4, # backtest, dont add values. its negative   -> SET TO -15 TO INCLUDE ALL

                                                    cds_tranche_tenor = ['5Y'],
                                                    # input options ['3Y','5Y','7Y','10Y']
                                                    cds_index_tenor=['3Y', '5Y', '7Y', '10Y'],
                                                    # input options ['3Y','5Y','7Y','10Y']

                                                    # NOT SET UP #
                                                    cds_index_product_type = ['indices', 'tranches'],
                                                    #input options ['Indices', 'Tranches']

    )

    #index_tranche_tool.index_tranche_pair_trade_analysis()




    # for back testing only
    back_test_instance = index_tranche_tool # for index maturity else filters out quotes historically

    back_test_instance.index_tranche_pair_trade_analysis() # UNHASH THISSSSSSSSSSSSS FOR NORMAL PROCESS
    #back_test_instance.index_tranche_rolldown_hedge_basis_analysis() #REMOVE this to run everything######################################

    # Run the back test files
    pair_trade_results = index_tranche_tool.back_test()






