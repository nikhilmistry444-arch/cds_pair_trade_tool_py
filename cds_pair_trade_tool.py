#####NOTES#####
# add process to clean the cds_raw_data source. have cleaned up manually 29/09/2023 by updating ref obs, and adding missing isins to bond secmaster
# smooth historic relationships for those that have been upgraded and downgraded. add rating change table and beta based on periods
# forward looking momentum should know current rating and see if the relationship is where it should be move from current levels
import cds_calculator
# Description: Main file to run the cds pair trade analysis
import cds_raw_data
import cds_raw_data_cache
import cds_momentum_signal
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
import live_data
import beta_adjustments

import pandas as pd
import numpy as np
import sys
import pickle
import os

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from scipy import stats
from dateutil.relativedelta import relativedelta

from xbbg import blp

def todays_date():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    return today

def cds_pair_trade_analysis(sector,industry_group, seniority, region, ig_hy, date_range_start, date_range_end, cds_1_tenor,
                            cds_2_tenor, beta_absolute_or_relative,net_carry_requirement,target_spread_requirement,
                            beta_historically_adjusted_adhoc,duration_weighted,rolldown_carry_months,cash_benchmarked,
                            forward_beta_adjusted,beta_historically_adjusted_attribute_changes):

    #Run Historic Data

    # extract all cds data ( currently T-1 Data from Data Source)
    all_cds_df_stage_one = cds_raw_data_cache.load_data(module=cds_raw_data,
                                                  function='cds_quote_data',
                                                  directory='historic_raw_data',
                                                  filename='all_cds_df_stage_one.pickle',
                                                  start_date=date_range_start,
                                                  end_date=date_range_end,
                                                  pricing_source=source)
    # filter all cds data + adjust ratings column
    all_cds_df_stage_two = cds_raw_data.filter_cds(df=all_cds_df_stage_one,
                                                 sector=sector,
                                                 industry_group=industry_group,
                                                 seniority=seniority,
                                                 region=region,
                                                 cds_1_tenor=cds_1_tenor,
                                                 cds_2_tenor=cds_2_tenor,
                                                 ig_hy=ig_hy)
    # ratings adjustment - takes worst rating for ticker/seniority day by day on all bonds. ref ob might not have been live in certain days in the past when it got set up
    all_cds_df_stage_three = cds_raw_data_cache.load_data(module=cds_raw_data,
                                                 function='historic_ratings_by_ticker_seniority',
                                                 directory='historic_raw_data',
                                                 filename='all_cds_df_stage_three.pickle',
                                                 start_date=date_range_start,
                                                 end_date=date_range_end,
                                                 data_frame=all_cds_df_stage_two)

    all_cds_df = all_cds_df_stage_three

    # horizontally adjust cds ticker's based on todays/current cds tickers attributes
    # checks for ratings changes and adjusts + 2 months rating lag adjustment
    if beta_historically_adjusted_attribute_changes == 'Y':
        all_cds_df = cds_raw_data_cache.load_data(module=variable_keys_beta,
                                                  function='run_processes',
                                                  directory='historic_raw_data',
                                                  filename='all_cds_df_stage_four.pickle',
                                                  start_date=date_range_start,
                                                  end_date=date_range_end,
                                                  data_frame=all_cds_df_stage_three,
                                                  process='beta_adjust_historical_attribute_changes_vs_now')

    # Run Today's/Current Live Quotes
        # and Repeat processes after historic data is cached to speed up time

    # find the latest interest rates swap curve - currently takes T-1(latest and most recent stored IRS curve. Find way to find the live version)
    interest_rates_swap_curves_df = cds_raw_data.interest_rates_swap_curves(end_date=date_range_end)
    # live quotes
    current_all_cds_df = live_data.cds_live_data(df=all_cds_df, date_range_end=date_range_end)

    # Combine Historic and Live/Current Data
    all_cds_df = pd.concat([all_cds_df, current_all_cds_df], ignore_index=True)
    # calculates historic beta's day by day given historical events. Just assigns a beta from 1 to (1*events). No impact to quote_y. Happens later
    if beta_historically_adjusted_adhoc == 'Y':
        #all_cds_df = variable_keys_beta.beta_overrides_by_date(df=all_cds_df)
        all_cds_df = beta_adjustments.beta_overrides_by_date(df=all_cds_df, method='CDS', end_date=date_range_end)

    # Generate full cds spread ranges df
    cds_spread_range_df = cds_raw_data.cds_spread_range_data(end_date=date_range_end, source=source)

    # add rolldown, carry and return analysis
    end_date = datetime.datetime.strptime(date_range_end, '%Y-%m-%d')
    date, date_tag = end_date + relativedelta(months=rolldown_carry_months), str(rolldown_carry_months) + 'm'
    date, end_date = pd.to_datetime(date), pd.to_datetime(end_date)
    carry_years_date = ((date - end_date).days) / 365.25
    cds_notes = 'no data'
    spread_quote_rolldown = 0
    current_all_cds_df = current_all_cds_df.drop_duplicates(subset=['bbg_cds_ticker', 'seniority', 'tenor', 'currency']).reset_index()
    current_all_cds_df = current_all_cds_df.drop('index', axis=1)
    current_all_cds_df['maturity_years'] = current_all_cds_df['tenor'].map(variable_keys_beta.tenor_to_year)

    for i in range(len(current_all_cds_df)):
        cds_currency = current_all_cds_df.loc[i,'currency']
        cds_maturity_years = variable_keys_beta.tenor_to_year[current_all_cds_df.loc[i,'tenor']]
        cds_quote = current_all_cds_df.loc[i,'quote']
        cds_coupon = current_all_cds_df.loc[i,'coupon']
        cds_bbg_ticker = current_all_cds_df.loc[i,'bbg_cds_ticker']
        cds_seniority = current_all_cds_df.loc[i,'seniority']

        # calculate return on upfront (use cds duration calculator)
        try:
            cds_upfront_amount = cds_calculator.cds_upfront_calculator(swap_curve_df=interest_rates_swap_curves_df,currency=cds_currency,maturity_years=cds_maturity_years,coupon=cds_coupon - cds_quote,end_date=end_date)
        except:
            cds_upfront_amount = 1

        if cash_benchmarked == 'Yes':
            cash_usage_carry = (cds_upfront_amount / 10000) * 400  # *carry_years_date
        else:
            cash_usage_carry = 0

        # cds curve table - create table by maturity order (linear rolldown)
        cds_curve_current_quote = current_all_cds_df[(current_all_cds_df['bbg_cds_ticker'] == cds_bbg_ticker) & (current_all_cds_df['currency'] == cds_currency) & (current_all_cds_df['seniority'] == cds_seniority)]
        cds_curve_current_quote = cds_curve_current_quote[['maturity_years', 'quote']]
        zero_year_row = pd.Series([0, 0], index=cds_curve_current_quote.columns)
        cds_curve_current_quote = cds_curve_current_quote.append(zero_year_row, ignore_index=True)
        cds_curve_current_quote = cds_curve_current_quote.sort_values(by=['maturity_years'],ascending=False)
        cds_curve_current_quote = cds_curve_current_quote.reset_index(drop=True)

        years_date = cds_maturity_years - carry_years_date
        duration_end_date = years_date
        duration_start_date = cds_maturity_years

        # calculate the rolldown+carry calculations
        if years_date < 0:  # don't annualize. assume same rate throughout the holding period
            current_all_cds_df.loc[i, str(date_tag) + ' R+C'] = round(cds_quote - cash_usage_carry, 2)
            current_all_cds_df.loc[i, str(date_tag) + ' R+C' + str(' % of Quote')] = round(((cds_quote - cash_usage_carry / cds_quote)), 2)
        else:
            for y in range(len(cds_curve_current_quote)):
                if cds_curve_current_quote['maturity_years'].iloc[y] == years_date:
                    spread_quote_rolldown = 0
                    break
                else:
                    if cds_curve_current_quote['maturity_years'].iloc[y] > years_date:
                        if cds_curve_current_quote['maturity_years'].iloc[y + 1] <= years_date:
                            bracket_spread = ((years_date - cds_curve_current_quote['maturity_years'].iloc[y + 1]) / (cds_curve_current_quote['maturity_years'].iloc[y] - cds_curve_current_quote['maturity_years'].iloc[y + 1])) * (cds_curve_current_quote['quote'].iloc[y] - cds_curve_current_quote['quote'].iloc[y + 1])
                            rolldown_spread_quote = cds_curve_current_quote['quote'].iloc[y + 1] + bracket_spread
                            spread_quote_rolldown = cds_quote - rolldown_spread_quote
                            current_all_cds_df.loc[i, str(date_tag) + ' R+C'] = round(((cds_quote * carry_years_date) + (spread_quote_rolldown * duration_end_date)) - cash_usage_carry, 0)
                            current_all_cds_df.loc[i, str(date_tag) + ' R+C' + str(' % of Quote')] = round((((((cds_quote * carry_years_date) + (spread_quote_rolldown * duration_end_date)) - cash_usage_carry) / cds_quote)),2)
                            break
                        else:
                            continue

        # calculate % of return
        try:
            current_all_cds_df.loc[i, str(date_tag) + ' R+C' + str(' % Rtn')] = round(((current_all_cds_df.loc[i, str(date_tag) + ' R+C']) / (cds_upfront_amount)) * 100)
        except:
            current_all_cds_df.loc[i, str(date_tag) + ' R+C' + str(' % Rtn')] = ''

        current_all_cds_df.loc[i, str(date_tag) + str(' Upfront(bps)')] = round(cds_upfront_amount)


    # - Pairing and Finding difference - #
    # cds_1 vs cds_2
    all_cds_df_1 = current_all_cds_df[(current_all_cds_df['tenor'] == cds_1_tenor)]
    cds_1_df_list = all_cds_df_1.drop_duplicates(subset=['bbg_cds_ticker', 'seniority','tenor'])

    all_cds_df_2 = current_all_cds_df[(current_all_cds_df['tenor'] == cds_2_tenor)]
    cds_2_df_list = all_cds_df_2.drop_duplicates(subset=['bbg_cds_ticker', 'seniority','tenor'])
    cds_pair_trade_results = []

    # spread range dataframe (cleans and extrapolates)
    spread_ranges_rating_cds = spread_ranges_generator.find_spread_ranges(cds_spread_range_df=cds_spread_range_df, sector=sector, seniority=seniority, region=region, tenor =[cds_1_tenor, cds_2_tenor])

    for index_1, cds_1_row in cds_1_df_list.iterrows():
        cds_1_bbg_ticker = cds_1_row['bbg_cds_ticker']
        cds_1_ticker = cds_1_row['ticker']
        cds_1_seniority = cds_1_row['seniority']
        cds_tenor_1 = cds_1_row['tenor']
        cds_1_currency = cds_1_row['currency']
        cds_1_RC = cds_1_row[f"{rolldown_carry_months}m R+C"]
        cds_1_upfront_bps = cds_1_row[f"{rolldown_carry_months}m Upfront(bps)"]

        cds_1_df = all_cds_df[(all_cds_df['bbg_cds_ticker'] == cds_1_bbg_ticker) & (all_cds_df['seniority'] == cds_1_seniority) & (all_cds_df['tenor'] == cds_tenor_1) & (all_cds_df['currency'] == cds_1_currency)]

        # to remove duplicate pairs in cds 2 dataframe
        cds_2_df_list_to_delete = cds_2_df_list[(cds_2_df_list['bbg_cds_ticker'] == cds_1_bbg_ticker) & (cds_2_df_list['seniority'] == cds_1_seniority) & (cds_2_df_list['tenor'] == cds_tenor_1)]
        cds_2_df_list = cds_2_df_list[~cds_2_df_list.isin(cds_2_df_list_to_delete)].dropna()

        # loop 2nd list to iterate and scan all possible pairs
        for index_2, cds_2_row in cds_2_df_list.iterrows():
            cds_2_bbg_ticker = cds_2_row['bbg_cds_ticker']
            cds_2_ticker = cds_2_row['ticker']
            cds_2_seniority = cds_2_row['seniority']
            cds_tenor_2 = cds_2_row['tenor']
            cds_2_currency = cds_2_row['currency']
            cds_2_RC = cds_2_row[f"{rolldown_carry_months}m R+C"]
            cds_2_upfront_bps = cds_2_row[f"{rolldown_carry_months}m Upfront(bps)"]

            if (cds_2_bbg_ticker + cds_2_seniority + cds_tenor_2 + cds_2_currency != cds_1_bbg_ticker + cds_1_seniority + cds_tenor_1 + cds_1_currency):
                cds_2_df = all_cds_df[(all_cds_df['bbg_cds_ticker'] == cds_2_bbg_ticker) & (all_cds_df['seniority'] == cds_2_seniority) & (all_cds_df['tenor'] == cds_tenor_2) & (all_cds_df['currency'] == cds_2_currency)]

                # combine both dataframes into one to pair
                cds_paired_df = cds_1_df.merge(cds_2_df, on='pricedate', how='inner', indicator=True)

                #need to improve. add cases where quote type is upfront
                if cds_paired_df.empty:
                    continue # run next pair since theres nothing to check

                # - Data Calculations
                    # contains all the historic data for a pair that has been modified

                #start the python class. initialise the original self. variables
                p1 = cds_calculator.cds_paired_dataframe_analyser(cds_paired_df, beta_historically_adjusted_adhoc, beta_absolute_or_relative, duration_weighted)

                # generate - beta_absolute_or_relative - finds beta ratio for each day
                    # regression to find flat beta over the historic periods data set
                    # assigns beta ratio for each day ( ad hoc beta * regression of historic data pairs(x,y))
                p1.beta_absolute_or_relative_function()
                cds_paired_df = p1.dataframe

                #TO DO - duration adjust
                # older method for duration. not working/incorporated at the minute
                if duration_weighted == 'Yes':
                    duration_ratio = variable_keys_beta.tenor_to_year[cds_paired_df['tenor_y']]/variable_keys_beta.tenor_to_year[cds_paired_df['tenor_x']]
                    #####NEED TO WORK OUT######
                    # duration_ratio isn't used at the minute

                # out variables of the dataframe. All calculations should be done earlier
                p1.final_output_variables()

                # - Momentum Calculations
                    #Given the current market, what factor -5 to 5 is the ticker/sector/country likely to outperform
                #dataframe_ranges,rating, sector, region, country_two_digit, ticker, seniority, tenor
                cds_1_momentum, cds_1_momentum_spread_move = cds_momentum_signal.ranking_tree(spread_dataframe_ranges = spread_ranges_rating_cds, sector=p1.cds_1_sector, region=p1.cds_1_region, country_two_digit=p1.cds_1_country_two_digit, ticker=p1.cds_1_ticker, rating=p1.cds_1_current_rating, seniority=p1.cds_1_seniority, tenor=cds_tenor_1)
                cds_2_momentum, cds_2_momentum_spread_move = cds_momentum_signal.ranking_tree(spread_dataframe_ranges = spread_ranges_rating_cds, sector=p1.cds_2_sector, region=p1.cds_2_region, country_two_digit=p1.cds_2_country_two_digit, ticker=p1.cds_2_ticker, rating=p1.cds_2_current_rating, seniority=p1.cds_2_seniority, tenor=cds_tenor_2)


                ############################################################# TO DO!!!!!!!!!!!!!!!!!!!!!!!!!!!

                # incorporate 'todays manually assigned betas' into trade ideas for forward looking momentum different from the beta calcualted from correlation
                if forward_beta_adjusted == 'Y':
                    sector_multiplier = variable_keys_beta.sector_multiplier.get(cds_1_row['sector'], 1)
                    seniority_multiplier = variable_keys_beta.seniority_multiplier.get(cds_1_row['seniority'], 1)
                    rating_multiplier = variable_keys_beta.rating_multiplier.get(cds_1_row['rating'], 1)
                    tenor_multiplier = variable_keys_beta.tenor_multiplier.get(cds_1_row['tenor'], 1)

                    cds_1_current_beta = (
                            sector_multiplier *
                            seniority_multiplier *
                            rating_multiplier *
                            tenor_multiplier
                    )

                    sector_multiplier = variable_keys_beta.sector_multiplier.get(cds_2_row['sector'], 1)
                    seniority_multiplier = variable_keys_beta.seniority_multiplier.get(cds_2_row['seniority'], 1)
                    rating_multiplier = variable_keys_beta.rating_multiplier.get(cds_2_row['rating'], 1)
                    tenor_multiplier = variable_keys_beta.tenor_multiplier.get(cds_2_row['tenor'], 1)

                    cds_2_current_beta = (
                            sector_multiplier *
                            seniority_multiplier *
                            rating_multiplier *
                            tenor_multiplier
                    )
                    # current beta
                    p1.current_beta_ratio_pair = cds_2_current_beta / cds_1_current_beta

                ##############################################
                # TO Do . Incorporate todays beta for a forward looking view in bps
                #p1.cds_1_current_quote_momentum_beta, p1.cds_2_current_quote_momentum_beta = cds_paired_df.iloc[0]['quote_x'], cds_paired_df.iloc[0]['quote_y_original']*(1 / momentum_p1.current_beta_ratio_pair)
                #cds_1_momentum_beta_move, cds_2_momentum_beta_move = p1.cds_1_current_quote_momentum_beta - p1.cds_1_current_quote,

                ############################################################

                # momentum cases
                if -0.1 <= cds_1_momentum <= 0.1:
                    cds_1_momentum = 0
                    cds_1_momentum_spread_chg = p1.cds_1_current_quote
                else:
                    if cds_1_momentum > 0:
                        cds_1_momentum_spread_chg = p1.cds_1_current_quote - cds_1_momentum_spread_move
                    else:
                        cds_1_momentum_spread_chg = p1.cds_1_current_quote + cds_1_momentum_spread_move

                if -0.1 <= cds_2_momentum <= 0.1:
                    cds_2_momentum = 0
                    cds_2_momentum_spread_chg = p1.cds_2_current_quote
                else:
                    if cds_2_momentum > 0:
                        cds_2_momentum_spread_chg = p1.cds_2_current_quote - (cds_2_momentum_spread_move * (1 / p1.current_beta_ratio_pair))
                    else:
                        cds_2_momentum_spread_chg = p1.cds_2_current_quote + (cds_2_momentum_spread_move * (1 / p1.current_beta_ratio_pair))

                p1.current_quote_diff_momentum = cds_1_momentum_spread_chg - cds_2_momentum_spread_chg
                current_momentum_of_spread_difference = p1.current_quote_diff - p1.current_quote_diff_momentum

                # - Evaluation based off relative value and current positioning makes sense
                # transaction cost
                cds_1_transaction_cost = transaction_cost_calculator.calculate_transaction_cost_bp(rating=p1.cds_1_current_rating, seniority=p1.cds_1_seniority, tenor=cds_tenor_1, sector=p1.cds_1_sector, product='CDS')
                cds_2_transaction_cost = transaction_cost_calculator.calculate_transaction_cost_bp(rating=p1.cds_2_current_rating, seniority=p1.cds_2_seniority, tenor=cds_tenor_2, sector=p1.cds_2_sector, product='CDS')
                transaction_cost_bp = (cds_1_transaction_cost + (cds_2_transaction_cost * (1 / p1.current_beta_ratio_pair)))

                # cds1 momentum = cds2 momentum
                ###add momentum range between cds1 vs cds 2 to classify as mean reversion
                if cds_1_momentum == cds_2_momentum:  # then history only is important
                    if abs(p1.z_score_current_of_difference) * p1.st_dev_of_difference > transaction_cost_bp:  # transaction cost filter based for mean reversion logic
                        if p1.z_score_current_of_difference > 0:
                            cds_1_bs, cds_2_bs, condition_reason_note = 'Sell Protection','Buy Protection', 'Historic - Mean Reversion'
                            comp_decomp_note = 'compression' if p1.current_quote_diff > 0 else 'decompression'
                            net_carry = p1.current_quote_diff
                            net_carry = net_carry - (cds_1_transaction_cost / 2) - (cds_2_transaction_cost / 2)

                            target_spread_pickup = abs(p1.current_quote_diff - p1.average_of_difference) - transaction_cost_bp

                            net_carry_roll_down = cds_1_RC-(cds_2_RC/p1.current_beta_ratio_pair)
                            net_carry_roll_down_return = (net_carry_roll_down/(cds_1_upfront_bps - (cds_2_upfront_bps/p1.current_beta_ratio_pair)))*100

                            cds_pair_trade_results.append(
                                [p1.cds_1_long_name,cds_1_bbg_ticker, cds_1_bs, p1.cds_1_seniority, cds_tenor_1,
                                 p1.cds_2_long_name,cds_2_bbg_ticker, cds_2_bs,p1.cds_2_seniority, cds_tenor_2,
                                 p1.current_beta_ratio_pair, condition_reason_note, comp_decomp_note,p1.percentile_current_of_difference,net_carry, target_spread_pickup, transaction_cost_bp,net_carry_roll_down,net_carry_roll_down_return])
                        else:
                            cds_1_bs , cds_2_bs , condition_reason_note = 'Buy Protection' ,'Sell Protection','Historic - Mean Reversion'
                            comp_decomp_note = 'decompression' if p1.current_quote_diff > 0 else 'compression'
                            net_carry = (-1 * p1.current_quote_diff) if p1.current_quote_diff >= 0 else (-1 * p1.current_quote_diff)
                            net_carry = net_carry - (cds_1_transaction_cost / 2) - (cds_2_transaction_cost / 2)

                            target_spread_pickup = abs(p1.current_quote_diff - p1.average_of_difference) - transaction_cost_bp
                            net_carry_roll_down= (cds_1_RC * -1) + (cds_2_RC / p1.current_beta_ratio_pair)
                            net_carry_roll_down_return = (net_carry_roll_down / ((cds_1_upfront_bps * -1) + (cds_2_upfront_bps / p1.current_beta_ratio_pair))) * 100

                            cds_pair_trade_results.append(
                                [p1.cds_1_long_name,cds_1_bbg_ticker, cds_1_bs, p1.cds_1_seniority, cds_tenor_1, p1.cds_2_long_name,cds_2_bbg_ticker, cds_2_bs, p1.cds_2_seniority, cds_tenor_2,
                                 p1.current_beta_ratio_pair, condition_reason_note, comp_decomp_note, p1.percentile_current_of_difference,net_carry, target_spread_pickup, transaction_cost_bp,net_carry_roll_down,net_carry_roll_down_return])

                # cds 1 momentum > cds 2 momentum
                elif cds_1_momentum > cds_2_momentum:
                    if abs(p1.current_quote_diff-p1.current_quote_diff_momentum) >= transaction_cost_bp: #transaction cost filter
                        cds_1_bs, cds_2_bs = 'Sell Protection', 'Buy Protection'
                        comp_decomp_note = 'compression' if p1.current_quote_diff > 0 else 'decompression'
                        net_carry = p1.current_quote_diff
                        net_carry = net_carry - (cds_1_transaction_cost / 2) - (cds_2_transaction_cost / 2)
                        target_spread_pickup = abs(p1.current_quote_diff_momentum - p1.average_of_difference) - transaction_cost_bp

                        net_carry_roll_down = cds_1_RC - (cds_2_RC / p1.current_beta_ratio_pair)
                        net_carry_roll_down_return = (net_carry_roll_down / (cds_1_upfront_bps - ((cds_2_upfront_bps * -1) / p1.current_beta_ratio_pair))) * 100

                        if (cds_1_momentum > 0 and cds_2_momentum > 0) or (0 >= cds_1_momentum > cds_2_momentum):
                            if current_momentum_of_spread_difference > 0: #spread change and momentum all in favour of direction
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Strong-Momentum(Spread+Factor) + Mean Reversion (CDS1>CDS2)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Momentum(Spread+Factor) against Mean (CDS1>CDS2)'
                            elif current_momentum_of_spread_difference == 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Likely Mean Reversion(Factor only) (CDS1>CDS2)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Unlikely Mean Reversion(Factor Only) (CDS1>CDS2)'
                            else: # current_momentum_of_spread_difference < 0 , spread change dominated even though cds1 factor larger
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Strong-Momentum(Spread only) + Mean Reversion (CDS1>CDS2)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Momentum(Spread only) against Mean'
                                comp_decomp_note = 'decompression' if p1.current_quote_diff > 0 else 'compression'
                        else:
                            # cds1>0>=cds2
                            if current_momentum_of_spread_difference > 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Very Strong-Momentum(Spread+Factor) + Mean Reversion'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Strong Momentum(Spread+Factor) against Mean'
                            elif current_momentum_of_spread_difference == 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Likely Mean Reversion(Factor Only) (CDS1>0>=CDS2)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Unlikely Mean Reversion(Factor Only) (CDS1>0>=CDS2)'
                            else: # current_momentum_of_spread_difference < 0
                                condition_reason_note = 'Impossible - Error with code, data, spreads should be opposed etc (CDS1>0>=CDS2)'
                                comp_decomp_note = 'Impossible - Error with code, data etc, spreads should be opposed etc (CDS1>0>=CDS2)'

                        cds_pair_trade_results.append([p1.cds_1_long_name,cds_1_bbg_ticker, cds_1_bs, p1.cds_1_seniority, cds_tenor_1, p1.cds_2_long_name,cds_2_bbg_ticker,cds_2_bs,p1.cds_2_seniority,
                                                       cds_tenor_2, p1.current_beta_ratio_pair, condition_reason_note, comp_decomp_note,p1.percentile_current_of_difference,net_carry, target_spread_pickup, transaction_cost_bp,net_carry_roll_down,net_carry_roll_down_return])

                    else:
                        continue #no viable trades

                else: # cds 1 momemtum < cds 2 momentum
                    # #reverse of case 1
                    if abs(p1.current_quote_diff - p1.current_quote_diff_momentum) >= transaction_cost_bp:  # transaction cost filter
                        cds_1_bs, cds_2_bs = 'Buy Protection', 'Sell Protection'
                        comp_decomp_note = 'decompression' if p1.current_quote_diff > 0 else 'compression'
                        net_carry = (-1 * p1.current_quote_diff) if p1.current_quote_diff >= 0 else (-1 * p1.current_quote_diff)
                        net_carry = net_carry - (cds_1_transaction_cost / 2) - (cds_2_transaction_cost / 2)
                        target_spread_pickup = abs(p1.current_quote_diff_momentum - p1.average_of_difference) - transaction_cost_bp

                        net_carry_roll_down = (cds_1_RC * -1) + (cds_2_RC / p1.current_beta_ratio_pair)
                        net_carry_roll_down_return = (net_carry_roll_down / ((cds_1_upfront_bps * -1) + (cds_2_upfront_bps / p1.current_beta_ratio_pair))) * 100

                        if (cds_2_momentum > 0 and cds_1_momentum > 0) or (0 >= cds_2_momentum > cds_1_momentum):
                            if current_momentum_of_spread_difference > 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Strong-Momentum(Spread only) + Mean Reversion (CDS1>CDS2)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Momentum(Spread only) against Mean'
                                comp_decomp_note = 'compression' if p1.current_quote_diff > 0 else 'decompression'
                            elif current_momentum_of_spread_difference == 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Unlikely Mean Reversion(Factor Only) (CDS2>CDS1)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Likely Mean Reversion(Factor Only) (CDS2>CDS1)'
                            else:  # current_momentum_of_spread_difference < 0
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Momentum(Spread+Factor) against Mean'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Strong-Momentum(Spread+Factor) + Mean Reversion'
                        else:
                            # cds2>0>=cds1
                            if current_momentum_of_spread_difference > 0:
                                condition_reason_note = 'Impossible - Error with code, data, spreads should be opposed etc (CDS2>0>=CDS1)'
                                comp_decomp_note = 'Impossible - Error with code, data, spreads should be opposed etc (CDS2>0>=CDS1)'
                            elif current_momentum_of_spread_difference == 0:
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Unlikely Mean Reversion(Factor only) (CDS2>0>=CDS1)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Likely Mean Reversion(Factor only) (CDS2>0>=CDS1)'
                            else:  # current_momentum_of_spread_difference < 0
                                if p1.z_score_current_of_difference > 0:
                                    condition_reason_note = 'Very Strong Momentum(Spread+Factor) against Mean (CDS2>0>=CDS1)'
                                if p1.z_score_current_of_difference <= 0:
                                    condition_reason_note = 'Very Strong-Momentum(Spread+Factor) + Mean Reversion (CDS2>0>=CDS1)'

                        cds_pair_trade_results.append(
                            [p1.cds_1_long_name,cds_1_bbg_ticker, cds_1_bs, p1.cds_1_seniority, cds_tenor_1, p1.cds_2_long_name,cds_2_bbg_ticker, cds_2_bs,
                             p1.cds_2_seniority, cds_tenor_2, p1.current_beta_ratio_pair, condition_reason_note, comp_decomp_note,
                             p1.percentile_current_of_difference,net_carry, target_spread_pickup, transaction_cost_bp,net_carry_roll_down,net_carry_roll_down_return])

                    else:
                        continue  # no viable trades

    results_df = pd.DataFrame(list(cds_pair_trade_results),
                              columns=['CDS1','CDS1 Ticker', 'CDS1 Trade', 'CDS1 Seniority', 'CDS1 Tenor', 'CDS2','CDS2 Ticker', 'CDS2 Trade',
                                       'CDS2 Seniority', 'CDS2 Tenor', 'Beta Ratio', 'Reason',
                                       'Type', 'Percentile','Net Carry','Target_Spread_Pickup','T Cost','R+C','R+C Rtn'])



    #clean the results
    results_df['Percentile'] = results_df['Percentile'].round(0)
    results_df['Beta Ratio'] = results_df['Beta Ratio'].round(1)
    results_df['Net Carry'] = results_df['Net Carry'].round(0)
    results_df['Target_Spread_Pickup'] = results_df['Target_Spread_Pickup'].round(0)
    results_df['T Cost'] = results_df['T Cost'].round(0)
    results_df['R+C'] = results_df['R+C'].round(0)
    results_df['R+C Rtn'] = results_df['R+C Rtn'].round(0)

    #filter results for carry
    if net_carry_requirement == 'positive':
        results_df = results_df[results_df['Net Carry'] >= 0]
    elif net_carry_requirement == 'negative':
        results_df = results_df[results_df['Net Carry'] < 0]
    else:
        results_df = results_df

    # filter results for target spread pickup
    results_df = results_df[results_df['Target_Spread_Pickup'] >= target_spread_requirement]

    # filter out results for negative beta!!!!. NEED TO INVESTIGATE. REGRESSION FORMULA PUSHES NEGATIVE BETA OUT
    results_df = results_df[results_df['Beta Ratio'] >= 0]

    return results_df

if __name__ == '__main__':

    ### inputs for cds ###
    date_range_start = '2022-09-01' #'2021-01-01'
    date_range_end = todays_date()
    sector = ['Consumer Staples','Consumer Discretionary','Energy','Utilities'] # 'Consumer Staples','Consumer Discretionary','Energy','Utilities','Government','All'
    industry_group = ['All'] #otherwise 'All'
    seniority = ['Senior'] # [Senior, Subordinated]
    region = ['ASIA', 'EMEA'] # ['ASIA', 'EMEA', 'AMERICAS', 'EM']
    ig_hy = 'ALL' #[IG, HY OR ALL]
    source = 'Bloomberg' #Markit

    # beta parameters
    beta_absolute_or_relative = 'relative'  # [relative, absolute] absolute(outright spreads vs spread) or relative(beta adjusted via correlation calculation)
    beta_historically_adjusted_adhoc = 'Y' # anything else is no # all macro events that impact sectors, regions etc
    beta_historically_adjusted_attribute_changes = 'Y' # anything else is no #rating changes, M&A, entity changes etc need to be modified historically else the beta from regression is mid led

    forward_beta_adjusted = 'N' # TO DO!!!!!!!!!!!!!

    cds_1_tenor = '5Y'
    cds_2_tenor = '5Y'
    # input options = ['6M','1Y','2Y','3Y','4Y','5Y','7Y','10Y']

    duration_weighted = 'No' #even if the pair trade stays unchanged, the products can cost you money from moves  --NEED TO WORK OUT HOW TO DO DURATION AND BETA TOGETHER
    #input options = anything else is No

    net_carry_requirement = 'all' # [positive, negative or anything else is all]
    target_spread_requirement = 10 # [number >=0], pickup post transaction cost adjustment
    rolldown_carry_months = 12 # enter number of months to find carry/rolldown
    cash_benchmarked = 'Yes' #'Yes' else everthing else is no. cash usage costs

    results = cds_pair_trade_analysis(sector,industry_group, seniority, region, ig_hy, date_range_start, date_range_end,
                                      cds_1_tenor, cds_2_tenor, beta_absolute_or_relative,net_carry_requirement,target_spread_requirement,
                                      beta_historically_adjusted_adhoc,duration_weighted,rolldown_carry_months,cash_benchmarked,forward_beta_adjusted,beta_historically_adjusted_attribute_changes)
    # export results in excel form
    if not os.path.exists('results_excel'):
        os.makedirs('results_excel')
    results.to_excel(os.path.join('results_excel', 'single_cds_pair_trades.xlsx'), index=False, sheet_name='single_cds_pair_trades')

    print(results)

