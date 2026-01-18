# Description: Main file to run cds_bonds_basis analysis

###TO DO###
##CHECK BOND ENTITIES MATCH CDS ENTITIES!!!!!!!!!!   ------ VW have many entities. but CDS entity is parent so covers all. Need to incorporate for reliability


import cds_raw_data
import cds_momentum_signal
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
from pandas.tseries.offsets import BMonthEnd
from pandas.tseries.offsets import BDay

import pandas as pd
import numpy as np
import sys

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from scipy import stats

def todays_date():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    return today

def historic_changes_dates(current_date):
    starting_date = pd.to_datetime(current_date)
    #1D
    one_day_ago = starting_date - pd.DateOffset(days=1)
    while one_day_ago.weekday() >= 5: # Mon-Fri are 0-4, 5-6 weekend
        one_day_ago = one_day_ago - BDay(1)
    one_day_ago = one_day_ago.strftime('%Y-%m-%d')
    #1W
    one_week_ago = starting_date - pd.DateOffset(weeks=1)
    while one_week_ago.weekday() >= 5: # Mon-Fri are 0-4, 5-6 weekend
        one_week_ago = one_week_ago - BDay(1)
    one_week_ago = one_week_ago.strftime('%Y-%m-%d')
    #1M
    one_month_ago = starting_date - pd.DateOffset(months=1)
    while one_month_ago.weekday() >= 5: # Mon-Fri are 0-4, 5-6 weekend
        one_month_ago = one_month_ago - BDay(1)
    one_month_ago= one_month_ago.strftime('%Y-%m-%d')
    #3M
    three_months_ago = starting_date - pd.DateOffset(months=3)
    while three_months_ago.weekday() >= 5: # Mon-Fri are 0-4, 5-6 weekend
        three_months_ago = three_months_ago - BDay(1)
    three_months_ago = three_months_ago.strftime('%Y-%m-%d')
    return one_day_ago, one_week_ago, one_month_ago, three_months_ago
def run_cds_bond_basis_analysis(cds_tenor, bond_portfolio, region, sector, basis_spread_requirement,positive_negative_basis,current_date,source):

    one_day_ago, one_week_ago, one_month_ago, three_months_ago = historic_changes_dates(current_date)

    #find all bond universe saved in database (closest to tenor and unique for multiple currencies)
    ###do later. not useful for our team at the minute
    ##will need to find way to scan unique and best bonds and closest to tenor of cds maturity date. based on closest to cds_maturiity_year

    #find portfolio bond universe
    portfolio_bond_universe = cds_raw_data.find_portfolio_bond_data(end_date=current_date,bond_portfolio=bond_portfolio,one_day_ago=one_day_ago,one_week_ago=one_week_ago,one_month_ago=one_month_ago,three_months_ago= three_months_ago)
    if 'All' not in sector:
        portfolio_bond_universe = portfolio_bond_universe.loc[(portfolio_bond_universe['Sector'].isin(sector))]
    if 'All' not in region:
        portfolio_bond_universe = portfolio_bond_universe.loc[(portfolio_bond_universe['Region'].isin(region))]

    ####BEST TO USE BLOOMBERG FOR SPREADS FOR ACCURACY########
    portfolio_bond_universe = portfolio_bond_universe[['Portfolio','ISIN','Ticker','SecName','Issuer','CCY','Region','Sector','OrigRating','par_value_source','Seniority','Perpetual','credit_duration','1D_Bond','1W_Bond','1M_Bond','3M_Bond']].reset_index()
    for i in range(len(portfolio_bond_universe)):
        if portfolio_bond_universe.loc[i, 'Seniority'] == 'Jr Sub':
            portfolio_bond_universe.loc[i, 'Seniority'] = 'Subordinated'
        elif portfolio_bond_universe.loc[i, 'Seniority'] == 'Sub':
            portfolio_bond_universe.loc[i, 'Seniority'] = 'Subordinated'
        elif portfolio_bond_universe.loc[i, 'Perpetual'] == 'Y' and portfolio_bond_universe.loc[i, 'Seniority'] == 'Sr Unsec':
            portfolio_bond_universe.loc[i, 'Seniority'] = 'Subordinated'
        else:
            portfolio_bond_universe.loc[i, 'Seniority'] = 'Senior'

    #find cds universe
    cds_quote_for_basis = cds_raw_data.cds_quote_for_basis(start_date=three_months_ago,end_date=current_date, source=source)
    cds_quote_for_basis = cds_quote_for_basis[cds_quote_for_basis['pricedate'].isin([one_day_ago, one_week_ago, one_month_ago, three_months_ago])]
    cds_quote_for_basis['quote_period'] = np.where(cds_quote_for_basis['pricedate'] == one_day_ago, '1D_CDS', np.where(cds_quote_for_basis['pricedate'] == one_week_ago, '1W_CDS', np.where(cds_quote_for_basis['pricedate'] == one_month_ago, '1M_CDS', '3M_CDS')))
    cds_quote_for_basis = pd.pivot_table(cds_quote_for_basis,index=['ticker', 'murex_name', 'seniority', 'bbg_cds_ticker', 'currency','tenor'], columns='quote_period', values='quote')
    cds_quote_for_basis = cds_quote_for_basis.reset_index()

    if (cds_tenor == 'all') or (cds_tenor == 'All') or (cds_tenor == 'ALL'):
        cds_tenor_list = cds_raw_data.find_unique_cds_tenor_list(one_day_ago)
        cds_tenor_list['cds_maturity_year'] = cds_tenor_list['cds_tenor'].map(variable_keys_beta.tenor_to_year)
        cds_tenor_list = cds_tenor_list.sort_values('cds_maturity_year', ascending=True).reset_index()

        bond_cds_results_df = pd.DataFrame()

        for cds_maturity_year in cds_tenor_list['cds_maturity_year']:
            maturity_low_range = cds_maturity_year - 0.5
            maturity_high_range = cds_maturity_year + 0.5
            portfolio_bond_universe_filtered = portfolio_bond_universe.loc[(portfolio_bond_universe['credit_duration'] >= maturity_low_range) & (portfolio_bond_universe['credit_duration'] <= maturity_high_range)]
            portfolio_bond_universe_filtered['cds_tenor'] = cds_tenor_list[cds_tenor_list['cds_maturity_year'] == cds_maturity_year]['cds_tenor'].values[0] # create new column tag

            #inner join portfolio bond universe with cds quote for basis. only care about data we have
            merged_dataframe_sub_results = pd.merge(portfolio_bond_universe_filtered, cds_quote_for_basis, how='inner', left_on=['Ticker','Seniority','CCY','cds_tenor'], right_on =['ticker','seniority','currency','tenor'])
            bond_cds_results_df = bond_cds_results_df.append(merged_dataframe_sub_results)

        bond_cds_results_df = bond_cds_results_df.reset_index()

    else:
        bond_cds_results_df = pd.DataFrame()
        # bond_universe = bond_universe.loc[(bond_universe['credit_duration'] >= maturity_low_range) & (bond_universe['credit_duration'] <= maturity_high_range)]
        cds_maturity_year = variable_keys_beta.tenor_to_year[cds_tenor]
        maturity_low_range = cds_maturity_year - 0.5
        maturity_high_range = cds_maturity_year + 0.5
        portfolio_bond_universe_filtered = portfolio_bond_universe.loc[(portfolio_bond_universe['credit_duration'] >= maturity_low_range) & (portfolio_bond_universe['credit_duration'] <= maturity_high_range)]
        portfolio_bond_universe_filtered['cds_tenor'] = cds_tenor  # create new column tag

        # inner join portfolio bond universe with cds quote for basis. only care about data we have
        merged_dataframe_sub_results = pd.merge(portfolio_bond_universe_filtered, cds_quote_for_basis, how='inner', left_on=['Ticker', 'Seniority', 'CCY', 'cds_tenor'], right_on=['ticker', 'seniority', 'currency', 'tenor'])
        bond_cds_results_df = bond_cds_results_df.append(merged_dataframe_sub_results)

        bond_cds_results_df = bond_cds_results_df.reset_index()

    #finalise the results
    bond_cds_results_df['1D'] = bond_cds_results_df['1D_CDS'] - bond_cds_results_df['1D_Bond'] if bond_cds_results_df['1D_Bond'] is not None else None
    bond_cds_results_df['1W'] = bond_cds_results_df['1W_CDS'] - bond_cds_results_df['1W_Bond'] if bond_cds_results_df['1W_Bond'] is not None else None
    bond_cds_results_df['1M'] = bond_cds_results_df['1M_CDS'] - bond_cds_results_df['1M_Bond'] if bond_cds_results_df['1M_Bond'] is not None else None
    bond_cds_results_df['3M'] = bond_cds_results_df['3M_CDS'] - bond_cds_results_df['3M_Bond'] if bond_cds_results_df['3M_Bond'] is not None else None

    bond_cds_results_df = bond_cds_results_df[['Portfolio','ISIN','Ticker','SecName','CCY','Region','Sector','credit_duration','cds_tenor','Seniority','1D_Bond','1D_CDS','1D','1W','1M','3M']]
    bond_cds_results_df['credit_duration'] = bond_cds_results_df['credit_duration'].round(2)
    bond_cds_results_df['1D_Bond'] = bond_cds_results_df['1D_Bond'].round(1)
    bond_cds_results_df['1D_CDS'] = bond_cds_results_df['1D_CDS'].round(1)
    bond_cds_results_df['1D'] = bond_cds_results_df['1D'].round(1)
    bond_cds_results_df['1W'] = bond_cds_results_df['1W'].round(1)
    bond_cds_results_df['1M'] = bond_cds_results_df['1M'].round(1)
    bond_cds_results_df['3M'] = bond_cds_results_df['3M'].round(1)
    bond_cds_results_df.rename(columns={'SecName':'Security','credit_duration': 'Bond Duration', 'cds_tenor':'CDS Tenor','1D_Bond':'1D_Bond_Spread','1D_CDS':'1D_CDS_Spread'}, inplace=True)



    if positive_negative_basis == 'Positive':
        bond_cds_results_df = bond_cds_results_df[bond_cds_results_df['1D'] > 0]
    elif positive_negative_basis == 'Negative':
        bond_cds_results_df = bond_cds_results_df[bond_cds_results_df['1D'] > 0]
    else:
        pass


    print(bond_cds_results_df)
    #to scout all 5Yr universe generally for positive and negative

    #to screen positive basis in portfolio

    #to screen negative basis in portfolio



if __name__ == '__main__':

    #inputs
    cds_tenor = 'All' # if all then run for everything

    bond_portfolio =f'BOND_AlPH','EM_BOND_ALPH','QUANT_BOND_ALPH'
    region = ['All'] #'AMERICAS', 'EMEA', 'ASIA', 'EM'
    sector = ['All'] #'Communication Services', 'Consumer Discretionary', 'Consumer Staples', 'Energy', 'Financials', 'Government', 'Health Care', 'Industrials', 'Information Technology', 'Materials', 'Real Estate', 'Utilities'

    basis_spread_requirement = 5
    positive_negative_basis = 'Positive' # [positive, negative or anything else is all]

    current_date = todays_date()
    source = 'Bloomberg' #Markit

    run_cds_bond_basis_analysis(cds_tenor, bond_portfolio, region, sector, basis_spread_requirement,positive_negative_basis,current_date, source)