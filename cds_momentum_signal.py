####direction of cds. tigther, wider, neutral
#if momentum > 0 then it refers to positive credit momentum. so multiplying spreads needs to be multipled by -1
import cds_raw_data
import variable_keys_beta
import pandas as pd
import sys
import os

directory = os.getcwd()
inputs_file_path = os.path.join(directory, 'inputs', 'inputs.xlsx')
sector_df = pd.read_excel(inputs_file_path, sheet_name='cds_score_sector')
region_df = pd.read_excel(inputs_file_path, sheet_name='cds_score_region')
country_df = pd.read_excel(inputs_file_path, sheet_name='cds_score_country_2_digit')
ticker_df = pd.read_excel(inputs_file_path, sheet_name='cds_score_ticker')

#score from -5 to 5
def sector_tilt_score_calculation(sector):
    df = sector_df
    if sector not in df['sector']:
        sector_score = 0
        sector_score_note = 'sector not in list'
    else:
        sector_score = df[df['sector'] == sector]['score'].values[0]
        sector_score_note = 'sector in list'
    return sector_score, sector_score_note

#score from -5 to 5
def region_score_calculation(region):
    df = region_df
    if region not in df['region'].values:
        region_score = 0
        region_score_note = 'region not in list'
    else:
        region_score = df[df['region'] == region]['score'].values[0]
        region_score_note = 'region in list'
    return region_score, region_score_note

#score from -5 to 5
def country_score_calculation(country_two_digit):
    df = country_df
    if country_two_digit not in df['country_two_digit'].values:
        country_score = 0
        country_score_note = 'country not in list'
    else:
        country_score = df[df['country_two_digit'] == country_two_digit]['score'].values[0]
        country_score_note = 'country in list'
    return country_score, country_score_note

#score from -5 to 5
def ticker_specific_score_calculation(ticker):
    df = ticker_df
    if ticker not in df['ticker'].values:
        ticker_score = 0
        ticker_score_note = 'ticker not in list'
    else:
        ticker_score = df[df['ticker'] == ticker]['score'].values[0]
        ticker_score_note = 'ticker in list'
    return ticker_score, ticker_score_note

# factor rankings and weighting
def ranking_tree(spread_dataframe_ranges,rating, sector, region, country_two_digit, ticker, seniority, tenor ):
    dataframe_ranges = spread_dataframe_ranges
    rating = rating
    sector = sector
    region = region
    country_two_digit = country_two_digit
    ticker = ticker
    seniority = seniority
    tenor = tenor
    tenor_year = variable_keys_beta.tenor_to_year[tenor]

    # filter spread ranges table
    try:
        dataframe_ranges = dataframe_ranges[(dataframe_ranges['sector'] == sector) & (dataframe_ranges['region'] == region)
                                        & (dataframe_ranges['seniority'] == seniority) & (dataframe_ranges['tenor'] == tenor)]
    except:
        print('missing dataframe cds spread ranges - likely missing collection of cds ratings')

    # ordering of importance and overrides
    # country -> region priority
    country_score, country_score_note = country_score_calculation(country_two_digit)
    if country_score_note == 'country not in list':
        final_country_region_score, region_score_note = region_score_calculation(region)
    else:
        final_country_region_score = country_score

    # other scores
    sector_tilt_score, sector_tilt_score_note = sector_tilt_score_calculation(sector)
    ticker_specific_score, ticker_specific_score_note = ticker_specific_score_calculation(ticker)

    # 0 to 1 range
    max_score = 5
    min_score = -5

    # assign weights
    # analysis on country impacts versus company credit
    if ticker_specific_score_note == 'ticker not in list':
        momentum_score = ((3/10) * final_country_region_score) + ((5/10) * sector_tilt_score) + ((2/10) * ticker_specific_score)
    else:
        momentum_score = ((2/10) * final_country_region_score) + ((2/10) * sector_tilt_score) + ((6/10) * ticker_specific_score)
    momentum = momentum_score / max_score

    # fraction of momentum on spread move based on market spread ranges
    try:
        current_rank, current_spread = dataframe_ranges[dataframe_ranges['rating'] == rating]['rating_rank'].values[0] , dataframe_ranges[dataframe_ranges['rating'] == rating]['quote'].values[0]
        tight_rank = current_rank - 1
        tight_spread = dataframe_ranges[dataframe_ranges['rating_rank'] == tight_rank]['quote'].values[0] if tight_rank > 0 else current_spread/10 #probably can improve for tails
        wide_rank = current_rank + 1
        wide_spread = dataframe_ranges[dataframe_ranges['rating_rank'] == wide_rank]['quote'].values[0] if wide_rank <= 23 else current_spread + 100 #probably can improve for tails

        if momentum > 0:
            momentum_spread_move = (current_spread - tight_spread) * momentum
        elif momentum < 0:
            momentum_spread_move = (wide_spread - current_spread) * abs(momentum)
        else:
            momentum_spread_move = 0
    except:
        print(f'{ticker} has a WR rating. Momentum spread move is based off a 15bps range. Please Check')
        if momentum > 0:
            momentum_spread_move = (15 * (tenor_year/5)) * momentum
        elif momentum < 0:
            momentum_spread_move = (15 * (tenor_year/5)) * abs(momentum)
        else:
            momentum_spread_move = 0
    #if theres a WR rating, it messes up the script!!!!!! Change the refob/update the ref ob so a rating is provided. Should fix it


    return momentum , momentum_spread_move
