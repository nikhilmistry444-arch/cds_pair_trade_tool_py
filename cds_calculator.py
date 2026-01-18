import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from scipy.optimize import minimize
import cds_raw_data
import variable_keys_beta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats
import cds_momentum_signal
import spread_ranges_generator
import beta_adjustments


#####can be improved to match the payment schedule of cds. for now will discount a year from now until the residual is left -atm its super close
def cds_upfront_calculator(swap_curve_df, currency, maturity_years, coupon, end_date):
    swap_curve_df = swap_curve_df[swap_curve_df['currency'] == currency].reset_index()
    current_date = datetime.strptime(end_date, "%Y-%m-%d") if not isinstance(end_date, datetime) else end_date #in timestamp
    end_date = current_date
    maturity_years = float(maturity_years)

    # coupon payment schedule
    coupon_frequency = 4
    coupon_day = 20
    coupon_months = [3,6,12]
    coupon = float(coupon)


    current_day, current_month, current_year = current_date.day, current_date.month, current_date.year

    # discount assuming cashflow received +3months from today. Taking closest rate. CAN IMPROVE!!!!!!!! CHECK BELOW
    pv, cumulative_year, cumulative_period = 0, 0, 0

    # find the closest month
    if datetime(current_year-1,12,coupon_day) <= current_date <= datetime(current_year,3,coupon_day):
        closest_month_diff = ((datetime(current_year,3,coupon_day) - end_date).days) / 365.25
    elif datetime(current_year,3,coupon_day) < current_date <= datetime(current_year,6,coupon_day):
        closest_month_diff = ((datetime(current_year,6,coupon_day) - end_date).days) / 365.25
    elif datetime(current_year,6,coupon_day) < current_date <= datetime(current_year,9,coupon_day):
        closest_month_diff = ((datetime(current_year,9,coupon_day) - end_date).days) / 365.25
    else:  # datetime(current_year,9,coupon_day) < current_date <=datetime(current_year,12,coupon_day)
        closest_month_diff = ((datetime(current_year,12,coupon_day) - end_date).days) / 365.25

    cumulative_year = closest_month_diff
    cumulative_period = 1

    # run payment dates every quarter thereafter first period
    while cumulative_year <= maturity_years:
        if cumulative_year > swap_curve_df.loc[0, 'tenor_years']:
            # find rate from swap curve table
            i=0
            while i < len(swap_curve_df['tenor_years']):
                if swap_curve_df.loc[i,'tenor_years'] < cumulative_year <= swap_curve_df.loc[i+1,'tenor_years']:
                    pv = pv + ((coupon/coupon_frequency) / ((1 + (((swap_curve_df.loc[i+1, 'quote'] / 100) / coupon_frequency))) ** cumulative_period))
                    break
                else:
                    i += 1
        else: # cumulative_year <= swap_curve_df.swap_curve_df.loc[0, 'tenor_year']. # taking overnight rate for the first payment date
            pv = pv + ((coupon / coupon_frequency) / ((1 + (((swap_curve_df.loc[0, 'quote'] / 100) / coupon_frequency))) ** cumulative_period))

        cumulative_year += 0.25
        cumulative_period += 1

    return pv

def calculate_position_ratio (beta_x, duration_x, beta_y, duration_y):
    # Define the objective function (we want to minimize this)
    def objective(x):
        size_x, size_y = x
        return (size_x * beta_x + size_y * beta_y) ** 2 + (size_x * duration_x + size_y * duration_y) ** 2

    # Initial guess for the position sizes
    x0 = [1, -1]

    # Call the optimizer
    result = minimize(objective, x0)

    # Return the optimized position sizes
    return result.x


class cds_index_tranche_rolldown_carry:
    def __init__(self, index_tranche_df, data_to_run_df, rolldown_date_data, end_date, cash_usage_carry, method_type):
        self.index_tranche_df = index_tranche_df
        self.data_to_run_df = data_to_run_df
        self.rolldown_date_data = rolldown_date_data # [date, date_tag]
        self.end_date = end_date
        self.cash_usage_carry = cash_usage_carry
        self.method_type = method_type

    def filtered_index_tranche_df(self):
        if 'Same Series' in self.method_type:
        # same series - create table for the same series by maturity order (linear rolldown)
            index_tranche_df_filtered = self.index_tranche_df[
                                                                        (self.index_tranche_df['index_short_name'] == self.data_to_run_df.index_short_name) &
                                                                        (self.index_tranche_df['index_series'] == self.data_to_run_df.index_series) &
                                                                        (self.index_tranche_df['attachment'] == self.data_to_run_df.attachment) &
                                                                        (self.index_tranche_df['detachment'] == self.data_to_run_df.detachment)]
            index_tranche_df_filtered = index_tranche_df_filtered[['index_maturity_years', 'spread_quote']]
            zero_year_row = pd.Series([0, 0], index=index_tranche_df_filtered.columns)
            index_tranche_df_filtered = index_tranche_df_filtered.append(zero_year_row,ignore_index=True)
            index_tranche_df_filtered = index_tranche_df_filtered.sort_values(by=['index_maturity_years'], ascending=False)
            index_tranche_df_filtered = index_tranche_df_filtered.reset_index(drop=True)

        elif 'Diff Series' in self.method_type:
            #Diff Series  - create table for the diff series by maturity order (linear rolldown)
            index_tranche_df_filtered = self.index_tranche_df[
                                                                        (self.index_tranche_df['index_short_name'] == self.data_to_run_df.index_short_name) &
                                                                        (self.index_tranche_df['attachment'] == self.data_to_run_df.attachment) &
                                                                        (self.index_tranche_df['detachment'] == self.data_to_run_df.detachment) &
                                                                        (self.index_tranche_df['index_maturity_years'] == self.data_to_run_df.index_maturity_years)]
            index_tranche_df_filtered = index_tranche_df_filtered[['index_maturity_years', 'spread_quote']]
            zero_year_row = pd.Series([0, 0], index=index_tranche_df_filtered.columns)
            index_tranche_df_filtered = index_tranche_df_filtered.append(zero_year_row,ignore_index=True)
            index_tranche_df_filtered = index_tranche_df_filtered.sort_values(by=['index_maturity_years'], ascending=False)
            index_tranche_df_filtered = index_tranche_df_filtered.reset_index(drop=True)
        else:
            index_tranche_df_filtered = []

        return index_tranche_df_filtered

    def rolldown_carry(self):
        index_tranche_df_filtered = cds_index_tranche_rolldown_carry.filtered_index_tranche_df(self)
        rolldown_date, rolldown_date_tag, end_date = pd.to_datetime(self.rolldown_date_data[0]), self.rolldown_date_data[1], pd.to_datetime(self.end_date)
        carry_years_date = ((rolldown_date - end_date).days) / 365.25
        years_date = self.data_to_run_df.index_maturity_years - carry_years_date
        duration_end_date = years_date
        duration_start_date = self.data_to_run_df.index_maturity_years

        index_tranche_notes = 'no data'
        index_spread_quote_rolldown = 0

        # calculate the rolldown+carry calculations
        if years_date < 0: # annualised
            years_multiplier = (1/years_date)
            rolldown_carry_output = (self.data_to_run_df.spread_quote - self.cash_usage_carry) * years_multiplier
            spread_quote_rolldown = self.data_to_run_df.spread_quote
            rolldown_carry_output_pct_of_quote = rolldown_carry_output / self.data_to_run_df.spread_quote
        else:
            for i in range(len(index_tranche_df_filtered)):
                if index_tranche_df_filtered['index_maturity_years'].iloc[i] == years_date:
                    index_spread_quote_rolldown = 0
                    rolldown_carry_output = self.data_to_run_df.spread_quote - self.cash_usage_carry
                    spread_quote_rolldown = self.data_to_run_df.spread_quote
                    rolldown_carry_output_pct_of_quote = rolldown_carry_output / self.data_to_run_df.spread_quote
                    break
                else:
                    if index_tranche_df_filtered['index_maturity_years'].iloc[i] > years_date:
                        if index_tranche_df_filtered['index_maturity_years'].iloc[i+1] <= years_date:
                            bracket_spread = ((years_date - index_tranche_df_filtered['index_maturity_years'].iloc[i+1]) / (index_tranche_df_filtered['index_maturity_years'].iloc[i] - index_tranche_df_filtered['index_maturity_years'].iloc[i+1])) * (index_tranche_df_filtered['spread_quote'].iloc[i] - index_tranche_df_filtered['spread_quote'].iloc[i+1])
                            rolldown_spread_quote = index_tranche_df_filtered['spread_quote'].iloc[i+1] + bracket_spread
                            spread_quote_rolldown = self.data_to_run_df.spread_quote - rolldown_spread_quote
                            rolldown_carry_output = ((self.data_to_run_df.spread_quote*carry_years_date) + (spread_quote_rolldown * duration_end_date)) - self.cash_usage_carry
                            rolldown_carry_output_pct_of_quote = rolldown_carry_output/self.data_to_run_df.spread_quote
                            break
                        else:
                            continue
        return rolldown_carry_output, rolldown_carry_output_pct_of_quote, spread_quote_rolldown, years_date

class cds_index_tranche_vs_cds_analysis:
    def __init__(self, all_cds_constituents_df, cds_current_quotes_df, data_to_run, backup_cds_current_quotes):
        self.all_cds_constituents_df = all_cds_constituents_df
        self.cds_current_quotes_df = cds_current_quotes_df
        self.data_to_run = data_to_run
        self.backup_cds_current_quotes = backup_cds_current_quotes

        # filter to find constituents for the index/tranche we are calculating for
        cds_index_tranche_cds_constituents = self.all_cds_constituents_df
        cds_index_tranche_cds_constituents = cds_index_tranche_cds_constituents[(cds_index_tranche_cds_constituents['index_short_name'] == data_to_run.index_short_name) &
                                                                                (cds_index_tranche_cds_constituents['index_series'] == data_to_run.index_series) &
                                                                                (cds_index_tranche_cds_constituents['index_version'] == data_to_run.index_version)]
                                                                                #(cds_index_tranche_cds_constituents['tenor'] == data_to_run.tenor)] doesnt matter
        cds_index_tranche_cds_constituents = pd.merge(cds_index_tranche_cds_constituents, self.cds_current_quotes_df, left_on=['cds_constituents', 'tenor'], right_on=['bbg_cds_ticker', 'tenor'], how='left')

        # skip for empty cds constituent data
        #CAN IMPROVE#
        ###### LOOKS LIKE CDS 10Yr and 5YR are mixed so need an exhaustive default list for all the diff cds tickers for the same issuer!!!!!
        if not cds_index_tranche_cds_constituents.empty:
            # apply backup cds quotes
            # #############IMPROVE modify cds to tranche/index data
            for i, row in cds_index_tranche_cds_constituents.iterrows():
                if (row['quote'] is None or np.isnan(row['quote'])):
                    try:
                        cds_index_tranche_cds_constituents.loc[i,'quote'] = backup_cds_current_quotes.loc[(backup_cds_current_quotes['bbg_cds_ticker'] == r['cds_constituents']) & (backup_cds_current_quotes['tenor'] == row['tenor']),'quote'].values[0]
                    except:
                        row['quote'] = 0 # CAN IMPROVE # there may be a live quote
                if (row['cds_weight'] == 0 or np.isnan(row['recovery_rate'])):
                    try:
                        cds_index_tranche_cds_constituents.loc[i,'recovery_rate'] = cds_defaults.loc[(cds_defaults['bbg_cds_ticker']) == row['cds_constituents'],'recovery_rate'].values[0] if cds_defaults.loc[(cds_defaults['bbg_cds_ticker'] == row['cds_constituents']),'recovery_rate'].values[0] is not None else backup_cds_current_quotes.loc[(backup_cds_current_quotes['bbg_cds_ticker'] == row['cds_constituents']) & (backup_cds_current_quotes['tenor'] == row['tenor']),'recovery_rate'].values[0]
                    except:
                        cds_index_tranche_cds_constituents.loc[i,'recovery_rate'] = 0.1

        # clean data
            # CAN IMPROVE - need exact recoveries, not 0.1 for those missing
        cds_index_tranche_cds_constituents.loc[cds_index_tranche_cds_constituents['quote'].isnull(), 'recovery_rate'] = 0.1
        cds_index_tranche_cds_constituents['quote'].fillna(0, inplace=True)
        cds_index_tranche_cds_constituents['weighted_quote'] = (cds_index_tranche_cds_constituents['cds_weight'] / (cds_index_tranche_cds_constituents['cds_weight'].sum())) * cds_index_tranche_cds_constituents['quote']

        # cds ordered from highest spread to lowest. required for the cumulative loss in calculating basis for tranches
        cds_index_tranche_cds_constituents.sort_values(by=['cds_weight','quote'], ascending=[True,False], inplace=True)
        cds_index_tranche_cds_constituents.reset_index(drop=True, inplace=True)

        # filter for defaulted cds that make up index/tranche - assumes CDS WEIGHT =0 for all defaults
        df_cds_weight_zero = cds_index_tranche_cds_constituents[cds_index_tranche_cds_constituents['cds_weight'] == 0]
        # find percentage of defaulted cds of the index
        if len(cds_index_tranche_cds_constituents) == 0:
            df_cds_weight_zero['cds_weight'] = 0 # CAN IMPROVE - missing index/tranche constituents. Can assign. But the series are old/iliquid
        else:
            df_cds_weight_zero['cds_weight'] = 1 / len(cds_index_tranche_cds_constituents['index_short_name'])

        self.cds_index_tranche_cds_constituents = cds_index_tranche_cds_constituents
        self.df_cds_weight_zero = df_cds_weight_zero


    def calculate_realised_loss_and_defaults(self):

        # calculate default count based off of cds weight = 0
            #- Find ISDA/cds determinations committees for exact ones
        default_count = len(self.df_cds_weight_zero['cds_constituents'])
        default_list = self.df_cds_weight_zero['cds_constituents'].values
        total_weight = self.cds_index_tranche_cds_constituents['cds_weight'].sum()  #aka index factor

        # calculate realise loss
        realised_cumulative_loss = ((1 - self.df_cds_weight_zero['recovery_rate']) * self.df_cds_weight_zero['cds_weight']).sum()
        try:
            realised_notional_loss = realised_cumulative_loss / total_weight
        except: #CAN IMPROVE - cant assume cds_weight = 1 if cds_weight sum is 0
            realised_notional_loss = realised_cumulative_loss / 1

        return default_count, default_list, realised_notional_loss

    # for tranches
    def calculate_basis(self):

        basis = ''
        basis_list = []
        basis_list_cds = []
        # for indices

        if ((self.data_to_run.attachment) == 0) and (int(self.data_to_run.detachment) == 1):
            basis = self.cds_index_tranche_cds_constituents['weighted_quote'].sum()
        else:
        # for tranches

            # CAN IMPROVE #
            #- does cds weight=0 mean defaulted
            #- find original weight for those defaulted

            # calculate cumulative loss (cds weight*recovery rate for loss given its out of 100)
            i=0
            self.cds_index_tranche_cds_constituents['cumulative_loss'] = 0
            while i <len(self.cds_index_tranche_cds_constituents['weighted_quote']):
                if (i ==0 and self.cds_index_tranche_cds_constituents.loc[i,'cds_weight']==0):
                    self.cds_index_tranche_cds_constituents.loc[i, 'cumulative_loss'] == (1 - self.cds_index_tranche_cds_constituents.loc[i, 'recovery_rate']) * self.df_cds_weight_zero[self.df_cds_weight_zero['cds_constituents'] == self.cds_index_tranche_cds_constituents.loc[i, 'cds_constituents']]['cds_weight'].values[0]

                elif (i !=0 and self.cds_index_tranche_cds_constituents.loc[i,'cds_weight']==0):
                    self.cds_index_tranche_cds_constituents.loc[i, 'cumulative_loss'] = self.cds_index_tranche_cds_constituents.loc[i - 1, 'cumulative_loss'] + (1 - self.cds_index_tranche_cds_constituents.loc[i, 'recovery_rate']) * self.df_cds_weight_zero[self.df_cds_weight_zero['cds_constituents'] == self.cds_index_tranche_cds_constituents.loc[i, 'cds_constituents']]['cds_weight'].values[0]

                elif (i==0 and self.cds_index_tranche_cds_constituents.loc[i,'cds_weight']!=0):
                    self.cds_index_tranche_cds_constituents.loc[i, 'cumulative_loss'] = ((1 - self.cds_index_tranche_cds_constituents.loc[i, 'recovery_rate']) * (self.cds_index_tranche_cds_constituents.loc[i, 'cds_weight']))
                else:
                    self.cds_index_tranche_cds_constituents.loc[i, 'cumulative_loss'] = self.cds_index_tranche_cds_constituents.loc[i-1, 'cumulative_loss'] + ((1-self.cds_index_tranche_cds_constituents.loc[i,'recovery_rate']) * (self.cds_index_tranche_cds_constituents.loc[i,'cds_weight']))
                i = i+1

            # finding basis on tranche given attachment, detachment
            i=0
            cumulative_weight_tranche=0
            total_quote=0

            while i <len(self.cds_index_tranche_cds_constituents['cumulative_loss']):
                if (self.cds_index_tranche_cds_constituents.loc[i,'cumulative_loss']>= self.data_to_run.attachment and self.cds_index_tranche_cds_constituents.loc[i,'cumulative_loss']<= self.data_to_run.detachment):
                    cumulative_weight_tranche = self.cds_index_tranche_cds_constituents.loc[i,'cds_weight'] + cumulative_weight_tranche
                    total_quote = self.cds_index_tranche_cds_constituents.loc[i,'weighted_quote'] + total_quote
                    basis = total_quote / cumulative_weight_tranche
                    if (pd.notna(self.cds_index_tranche_cds_constituents.loc[i,'bbg_cds_ticker']) and self.cds_index_tranche_cds_constituents.loc[i,'cds_weight']!=0):
                        basis_list_cds.append(self.cds_index_tranche_cds_constituents.loc[i,'bbg_cds_ticker']) #need to not include defaulted tickers!!!!!! #needs fixing
                    i += 1
                else:
                    if self.cds_index_tranche_cds_constituents.loc[i,'cumulative_loss'] > self.data_to_run.detachment:
                        break
                    i += 1

            for bbg_cds_ticker in basis_list_cds:
                find_cds_weight = self.cds_index_tranche_cds_constituents.loc[self.cds_index_tranche_cds_constituents['bbg_cds_ticker']==bbg_cds_ticker,'cds_weight'].values[0]
                basis_list.append(f'({round(find_cds_weight/cumulative_weight_tranche*100,1)}-{bbg_cds_ticker})')

        self.basis_list_cds = basis_list_cds
        return basis, basis_list

    def calculate_momentum_index_tranche_cds_by_cds(self, source):

        # in case index/tranche constituents are wrong/mixed (found duplicates with different tenors)
        tenor = min(variable_keys_beta.tenor_to_year.keys(), key=lambda x: abs(variable_keys_beta.tenor_to_year[x] - (((pd.to_datetime(self.data_to_run['index_maturity']) - datetime.now()).days) / 365.25)))
        self.cds_index_tranche_cds_constituents = self.cds_index_tranche_cds_constituents[self.cds_index_tranche_cds_constituents['tenor'] == tenor]

        # remove defaulted cds because the cds weight is 0. find new weights and weighted quotes
        self.cds_index_tranche_cds_constituents = self.cds_index_tranche_cds_constituents[self.cds_index_tranche_cds_constituents['cds_weight'] != 0]
        if len(self.basis_list_cds) == 0:
            self.cds_index_tranche_cds_constituents = self.cds_index_tranche_cds_constituents
        else:
            self.cds_index_tranche_cds_constituents = self.cds_index_tranche_cds_constituents[self.cds_index_tranche_cds_constituents['cds_constituents'].isin(self.basis_list_cds)]

        self.cds_index_tranche_cds_constituents['cds_weight'] = self.cds_index_tranche_cds_constituents['cds_weight'] / float(self.cds_index_tranche_cds_constituents['cds_weight'].sum())
        self.cds_index_tranche_cds_constituents['weighted_quote'] = self.cds_index_tranche_cds_constituents['cds_weight'] / self.cds_index_tranche_cds_constituents['quote']
        self.cds_index_tranche_cds_constituents = self.cds_index_tranche_cds_constituents.reset_index(drop=True)

        momentum = (self.cds_index_tranche_cds_constituents['momentum'] * self.cds_index_tranche_cds_constituents['cds_weight']).sum()
        momentum_spread = (self.cds_index_tranche_cds_constituents['momentum_spread'] * self.cds_index_tranche_cds_constituents['cds_weight']).sum()
        return momentum, momentum_spread

class cds_paired_dataframe_analyser:
    def __init__(self, dataframe, beta_historically_adjusted, beta_absolute_or_relative, duration_weighted):
        # modify dataframe

        dataframe['pricedate'] = pd.to_datetime(dataframe['pricedate'])
        dataframe = dataframe.sort_values('pricedate', ascending=False)
        dataframe['quote_y_original'] = dataframe['quote_y']

        self.dataframe = dataframe
        self.beta_historically_adjusted = beta_historically_adjusted
        self.beta_absolute_or_relative = beta_absolute_or_relative
        self.duration_weighted = duration_weighted

    def beta_absolute_or_relative_function(self):

        # apply the betas to clean the data historically. Smoothing process so history can represent the present (adhoc and name specific beta adjustments done earlier)
        # required to make sure the new beta calculation below picks this up!
        self.dataframe['beta_ratio'] = self.dataframe['beta_y'] / self.dataframe['beta_x']
        self.dataframe['quote_y'] = self.dataframe['quote_y'] * (1 / self.dataframe['beta_ratio'])

        # find regression on adjusted quotes y for the accurate beta - beta from historic data
        if self.beta_absolute_or_relative == 'relative':
            slope, intercept, r_value, p_value, std_err = stats.linregress(self.dataframe['quote_x'],self.dataframe['quote_y'])
            self.dataframe['beta_y'] = slope * self.dataframe['beta_y']
        else:
            current_beta_ratio_pair = 1

        # assign base beta to quotes after adjustments to historic quotes
        self.dataframe['beta_ratio'] = self.dataframe['beta_y'] / self.dataframe['beta_x']
        self.dataframe['quote_y'] = self.dataframe['quote_y_original'] * (1 / self.dataframe['beta_ratio'])
        self.dataframe['quote_diff'] = self.dataframe['quote_x'] - self.dataframe['quote_y']
        self.current_beta_ratio_pair = self.dataframe.iloc[0]['beta_y'] / self.dataframe.iloc[0]['beta_x']


    def final_output_variables(self):
        # assigning values from dataframe
        self.cds_1_current_quote, self.cds_2_current_quote = self.dataframe.iloc[0]['quote_x'], self.dataframe.iloc[0]['quote_y']
        self.current_quote_diff = self.cds_1_current_quote - self.cds_2_current_quote
        self.cds_1_current_rating, self.cds_2_current_rating = self.dataframe.iloc[0]['rating_x'], self.dataframe.iloc[0]['rating_y']
        self.cds_1_long_name, self.cds_2_long_name = self.dataframe.iloc[0]['murex_name_x'], self.dataframe.iloc[0]['murex_name_y']
        self.cds_1_seniority, self.cds_2_seniority = self.dataframe.iloc[0]['seniority_x'], self.dataframe.iloc[0]['seniority_y']
        self.cds_1_sector, self.cds_2_sector = self.dataframe.iloc[0]['sector_x'], self.dataframe.iloc[0]['sector_y']
        self.cds_1_region, self.cds_2_region = self.dataframe.iloc[0]['region_x'], self.dataframe.iloc[0]['region_y']
        self.cds_1_ticker, self.cds_2_ticker = self.dataframe.iloc[0]['ticker_x'], self.dataframe.iloc[0]['ticker_y']
        self.cds_1_country_two_digit, self.cds_2_country_two_digit = self.dataframe.iloc[0]['country_x'], self.dataframe.iloc[0]['country_y']

        # statistics on the difference - based on current data - initial time frame
        self.average_of_difference = self.dataframe['quote_diff'].mean()
        self.st_dev_of_difference = self.dataframe['quote_diff'].std()
        self.z_score_series = stats.zscore(self.dataframe['quote_diff'])
        self.z_score_current_of_difference = self.z_score_series.values[0]
        self.percentile_10th_of_difference = np.percentile(self.dataframe['quote_diff'], 10)
        self.percentile_90th_of_difference = np.percentile(self.dataframe['quote_diff'], 90)
        self.percentile_current_of_difference = stats.percentileofscore(self.dataframe['quote_diff'], self.current_quote_diff)


class index_tranche_paired_dataframe_analyser:

    def __init__(self,data_start_date, start_date, end_date,pair_set_1,latest_version_series_df,latest_version_df, beta_data_filter):
        self.data_start_data = data_start_date
        self.start_date = start_date
        self.end_date = end_date
        self.pair_set_1 = pair_set_1
        self.latest_version_series_df = latest_version_series_df
        self.latest_version_df = latest_version_df
        self.beta_data_filter = beta_data_filter


        # apply beta data filter
        if beta_data_filter in ['Y', 'y', 'yes', 'Yes']:
            self.latest_version_series_df = beta_adjustments.beta_overrides_by_date(self.latest_version_series_df, method='index', end_date=self.end_date)
            self.latest_version_df = beta_adjustments.beta_overrides_by_date(self.latest_version_df, method='index', end_date=self.end_date)


        # latest_version only quotes - pair_set_1
        self.filter_quotes_historic_1_df = self.latest_version_df[
            (self.latest_version_df['index_short_name'] == pair_set_1['index_short_name']) &
            (self.latest_version_df['index_series'] == pair_set_1['index_series']) &
            (self.latest_version_df['attachment'] == pair_set_1['attachment']) &
            (self.latest_version_df['detachment'] == pair_set_1['detachment']) &
            (self.latest_version_df['tenor'] == pair_set_1['tenor'])]
        pair_set_1['start_pricing_date'] = pd.to_datetime(self.filter_quotes_historic_1_df['pricedate'].min())
        self.start_pricing_date_1 = pair_set_1['start_pricing_date'].to_pydatetime()

        # latest version and series per tenor
        self.rolling_tenor_pair_set_1 = self.latest_version_series_df[
            (self.latest_version_series_df['tenor'] == pair_set_1['tenor']) &
            (self.latest_version_series_df['index_short_name'] == pair_set_1['index_short_name']) &
            (self.latest_version_series_df['attachment'] == pair_set_1['attachment']) &
            (self.latest_version_series_df['detachment'] == pair_set_1['detachment'])]

    def beta_historical_regression_function(self, pair_set_2,duration_weighted,beta_calculation_method, beta_sub_method):

        # minimum threshold to run beta regression analysis on for viable trades

        min_number_of_date_threshold_for_beta = 45

        # find start_pricing_date_2
        filter_quotes_historic_2_df = self.latest_version_df[
            (self.latest_version_df['index_short_name'] == pair_set_2['index_short_name']) &
            (self.latest_version_df['index_series'] == pair_set_2['index_series']) &
            (self.latest_version_df['attachment'] == pair_set_2['attachment']) &
            (self.latest_version_df['detachment'] == pair_set_2['detachment']) &
            (self.latest_version_df['tenor'] == pair_set_2['tenor'])]
        pair_set_2['start_pricing_date'] = pd.to_datetime(filter_quotes_historic_2_df['pricedate'].min())
        start_pricing_date_2 = pair_set_2['start_pricing_date'].to_pydatetime()

        # latest version and series per tenor
        rolling_tenor_pair_set_2 = self.latest_version_series_df[
            (self.latest_version_series_df['tenor'] == pair_set_2['tenor']) &
            (self.latest_version_series_df['index_short_name'] == pair_set_2['index_short_name']) &
            (self.latest_version_series_df['attachment'] == pair_set_2['attachment']) &
            (self.latest_version_series_df['detachment'] == pair_set_2['detachment'])]

        # compare start_date, start_pricing_date_1 and start_pricing_date_2 -> start from the newest line
        start_date_datetime = pd.to_datetime(self.start_date)
        if (start_date_datetime <= self.start_pricing_date_1 or start_date_datetime <= start_pricing_date_2):
            start_pricing_datetime = max(self.start_pricing_date_1, start_pricing_date_2)
            start_pricing_date = start_pricing_datetime.strftime('%Y-%m-%d')
        else:
            start_pricing_date = start_date_datetime.strftime('%Y-%m-%d')

        # historical quotes where pricedates for each data set overlap
        pair_set_1_exact_historic_quotes = self.filter_quotes_historic_1_df[self.filter_quotes_historic_1_df['pricedate'] >= start_pricing_date]
        pair_set_2_exact_historic_quotes = filter_quotes_historic_2_df[filter_quotes_historic_2_df['pricedate'] >= start_pricing_date]

        # initialise beta calculator
        b1 = variable_keys_beta.calculate_beta

        if duration_weighted in ['Yes','Y','YES','y']:
            duration_1 = pair_set_1['index_maturity_years']
            duration_2 = pair_set_2['index_maturity_years']

            beta_ratio_pair = abs(duration_2 / duration_1)
            pair_set_1_quotes = pair_set_1_exact_historic_quotes
            pair_set_2_quotes = pair_set_2_exact_historic_quotes
        else:
            if beta_calculation_method == 'historic':
                if beta_sub_method == 'exact_to_exact':

                    # fix the length of the data so they match(likely remove dates from the inner join if data for one and not other)
                    pricedate_key = pair_set_1_exact_historic_quotes.merge(pair_set_2_exact_historic_quotes, on=['pricedate'], how='inner')
                    if len(pricedate_key) >= min_number_of_date_threshold_for_beta:
                        pricedate_key = pricedate_key['pricedate']
                        pair_set_1_exact_historic_quotes = pair_set_1_exact_historic_quotes[pair_set_1_exact_historic_quotes['pricedate'].isin(pricedate_key)].reset_index()
                        pair_set_2_exact_historic_quotes = pair_set_2_exact_historic_quotes[pair_set_2_exact_historic_quotes['pricedate'].isin(pricedate_key)].reset_index()

                        if len(pair_set_1_exact_historic_quotes['pricedate']) != len(pair_set_2_exact_historic_quotes['pricedate']):
                            print(index_short_name_1, index_series_1,f"{index_attachment_1}-{index_detachment_1}",len(pair_set_1_exact_historic_quotes['pricedate']),index_short_name_2, index_series_2,f"{index_attachment_2}-{index_detachment_2}",len(pair_set_2_historic_quotes['pricedate']),'historic data not same length')
                            beta_ratio_pair = 0 # check this case
                        else:
                            pair_set_1_exact_historic_quotes_beta = pair_set_1_hispair_set_1_exact_historic_quotestoric_quotes[['pricedate', 'spread_quote']]
                            pair_set_2_exact_historic_quotes_beta = pair_set_2_exact_historic_quotes[['pricedate', 'spread_quote']]
                            dod_diff_data_sets = pair_set_1_exact_historic_quotes_beta.merge(pair_set_2_exact_historic_quotes_beta,on=['pricedate'], how='inner')

                            # delete if quote T-1 is the same as T, to work out YChg/XChg for Beta
                            delete_list =[]
                            for i in range(len(dod_diff_data_sets)):
                                if i == 0:
                                    continue
                                else:
                                    if dod_diff_data_sets.loc[i,'spread_quote_x'] == dod_diff_data_sets.loc[i-1,'spread_quote_x']:
                                        delete_list.append(i)
                                    else:
                                        continue
                            dod_diff_data_sets = dod_diff_data_sets.drop(delete_list)
                            dod_diff_data_sets = dod_diff_data_sets.reset_index(drop=True)
                            dod_diff_data_sets['ratio_dod'] = 1

                            beta_ratio_pair = b1.for_index_tranches(method='regression', data_set=dod_diff_data_sets)
                            pair_set_1_quotes = pair_set_1_exact_historic_quotes
                            pair_set_2_quotes = pair_set_2_exact_historic_quotes
                    else:
                        beta_ratio_pair = 0

                elif beta_sub_method == 'rolling_tenor':

                    paired_historic_quotes = self.rolling_tenor_pair_set_1.merge(rolling_tenor_pair_set_2, on =['pricedate'] ,how='inner')
                    if len(paired_historic_quotes['pricedate']) >= min_number_of_date_threshold_for_beta:
                        beta_ratio_pair = b1.for_index_tranches(method='regression', data_set=paired_historic_quotes)
                        pair_set_1_quotes = pair_set_1_exact_historic_quotes
                        pair_set_2_quotes = pair_set_2_exact_historic_quotes
                    else:
                        beta_ratio_pair = 0

                elif beta_sub_method == 'rolling_tenor_to_exact_to_exact':

                    # create historic quotes from exact series and rolling tenor - pair set 1
                    rolling_tenor_pair_set_1 = self.rolling_tenor_pair_set_1[self.rolling_tenor_pair_set_1['pricedate'] < start_pricing_date]
                    pair_set_1_combined_historic_quotes = pd.concat([pair_set_1_exact_historic_quotes, rolling_tenor_pair_set_1], ignore_index=True)

                    # create historic quotes from exact series and rolling tenor - pair set 2
                    rolling_tenor_pair_set_2 = rolling_tenor_pair_set_2[rolling_tenor_pair_set_2['pricedate'] < start_pricing_date]
                    pair_set_2_combined_historic_quotes = pd.concat([pair_set_2_exact_historic_quotes, rolling_tenor_pair_set_2], ignore_index=True)
                    paired_historic_quotes = pair_set_1_combined_historic_quotes.merge(pair_set_2_combined_historic_quotes, on=['pricedate'], how='inner')
                    if len(paired_historic_quotes['pricedate']) >= min_number_of_date_threshold_for_beta:
                        beta_ratio_pair = b1.for_index_tranches(method='regression', data_set=paired_historic_quotes)
                        pair_set_1_quotes = pair_set_1_combined_historic_quotes
                        pair_set_2_quotes = pair_set_2_combined_historic_quotes
                    else:
                        beta_ratio_pair = 0

                else:
                    beta_ratio_pair = 1
                    pair_set_1_quotes = pair_set_1_exact_historic_quotes
                    pair_set_2_quotes = pair_set_2_exact_historic_quotes

            elif beta_calculation_method == 'cds_by_cds':
                print('todo')
                beta_ratio_pair = 1
                pair_set_1_quotes = pair_set_1_exact_historic_quotes
                pair_set_2_quotes = pair_set_2_exact_historic_quotes
            else:
                beta_ratio_pair = 1
                pair_set_1_quotes = pair_set_1_exact_historic_quotes
                pair_set_2_quotes = pair_set_2_exact_historic_quotes

        if beta_ratio_pair!=0:
            pair_set_2_exact_historic_quotes['spread_quote_y_beta'] = pair_set_2_exact_historic_quotes['spread_quote'] * (1 / beta_ratio_pair)


        self.beta_ratio_pair = beta_ratio_pair
        self.pair_set_1_historic_quotes = pair_set_1_exact_historic_quotes
        self.pair_set_2_historic_quotes = pair_set_2_exact_historic_quotes

        return self.beta_ratio_pair, self.pair_set_1_historic_quotes, self.pair_set_2_historic_quotes


    def combined_output_variables(self):
        pair_set_historic_quotes = self.pair_set_1_historic_quotes.merge(self.pair_set_2_historic_quotes, on=['pricedate'],how='inner')

        try:
            pair_set_historic_quotes = pair_set_historic_quotes.sort_values('pricedate', ascending=False)
            pair_set_historic_quotes['spread_quote_diff'] = pair_set_historic_quotes['spread_quote_x'] - pair_set_historic_quotes['spread_quote_y_beta']
            self.index_1_current_quote, self.index_2_current_quote = pair_set_historic_quotes.iloc[0]['spread_quote_x'], pair_set_historic_quotes.iloc[0]['spread_quote_y_beta']
            self.current_quote_diff = self.index_1_current_quote - self.index_2_current_quote
        except:
            print(pair_set_historic_quotes)


        # z scores on x and y to work out favourable direction
        # x (pair set 1)
        self.average_of_x = pair_set_historic_quotes['spread_quote_x'].mean()
        self.st_dev_of_x = pair_set_historic_quotes['spread_quote_x'].std()
        self.z_score_series_x = stats.zscore(pair_set_historic_quotes['spread_quote_x'])
        self.z_score_current_x = self.z_score_series_x.values[0]
        # y (pair set 2)
        self.average_of_y = pair_set_historic_quotes['spread_quote_y'].mean()
        self.st_dev_of_y = pair_set_historic_quotes['spread_quote_y'].std()
        self.z_score_series_y = stats.zscore(pair_set_historic_quotes['spread_quote_y'])
        self.z_score_current_y = self.z_score_series_y.values[0]
            # beta adjusted y (pair set 2)
        self.average_of_y_beta_adj = pair_set_historic_quotes['spread_quote_y_beta'].mean()
        self.st_dev_of_y_beta_adj = pair_set_historic_quotes['spread_quote_y_beta'].std()
        self.z_score_series_y_beta_adj = stats.zscore(pair_set_historic_quotes['spread_quote_y_beta'])
        self.z_score_current_y_beta_adj = self.z_score_series_y_beta_adj.values[0]


        # statistics on the difference - (based on current data - initial time frame) - x and beta adjusted y
        self.average_of_difference = pair_set_historic_quotes['spread_quote_diff'].mean()
        self.st_dev_of_difference = pair_set_historic_quotes['spread_quote_diff'].std()
        self.z_score_series = stats.zscore(pair_set_historic_quotes['spread_quote_diff'])
        self.z_score_current_of_difference = self.z_score_series.values[0]
        self.percentile_10th_of_difference = np.percentile(pair_set_historic_quotes['spread_quote_diff'], 10)
        self.percentile_90th_of_difference = np.percentile(pair_set_historic_quotes['spread_quote_diff'], 90)
        self.percentile_current_of_difference = stats.percentileofscore(pair_set_historic_quotes['spread_quote_diff'], self.current_quote_diff)

    def momentum_adjustor(self, index_1_momentum, index_1_momentum_spread, index_2_momentum, index_2_momentum_spread, beta_ratio_pair):

        # BELOW NEEDS TO GET FILLED OUT - IMPROVE!!!!!!!!!
        self.index_1_momentum, self.index_1_momentum_spread_move = index_1_momentum, index_1_momentum_spread
        self.index_2_momentum, self.index_2_momentum_spread_move, self.index_2_momentum_spread_beta_adj = index_2_momentum, index_2_momentum_spread, index_2_momentum_spread/beta_ratio_pair


        # non meaningful momentum #CAN REMOVE THE BELOW
        if -0.1 <= self.index_1_momentum <= 0.1:
            self.index_1_momentum = 0
            self.index_1_momentum_spread_move = 0
            self.index_1_momentum_current_quote = self.index_1_current_quote
            self.index_1_momentum_sign = 0
        else:
            if self.index_1_momentum > 0:
                self.index_1_momentum_current_quote = self.index_1_current_quote - self.index_1_momentum_spread_move
            else:
                self.index_1_momentum_current_quote = self.index_1_current_quote + self.index_1_momentum_spread_move
            self.index_1_momentum_sign = abs(self.index_1_momentum)/self.index_1_momentum

        if -0.1 <= self.index_2_momentum <= 0.1:
            self.index_2_momentum = 0
            self.index_2_momentum_spread_move = 0
            self.index_2_momentum_spread_beta_adj = 0
            self.index_2_momentum_current_quote = self.index_2_current_quote
            self.index_2_momentum_sign = 0
        else:
            if self.index_2_momentum > 0:
                self.index_2_momentum_current_quote = self.index_2_current_quote - (self.index_2_momentum_spread_move * (1 / beta_ratio_pair))
            else:
                self.index_2_momentum_current_quote = self.index_2_current_quote + (self.index_2_momentum_spread_move * (1 / beta_ratio_pair))
            self.index_2_momentum_sign = abs(self.index_2_momentum) / self.index_2_momentum

        self.current_quote_diff_momentum = self.index_1_momentum_current_quote - self.index_2_momentum_current_quote