import variable_keys_beta
import beta_adjustments
import datetime
import pyodbc
import sys
import logging
import pandas as pd

import live_data

###SQL Conection###
def sql_connect():
    dbConnectionStr = 'DRIVER={SQL Server};SERVER=SHA2-DBS-01.pr.cppib.ca,20081;Database=SHA2;Trusted_Connection=True;'
    try:
        connection = pyodbc.connect(dbConnectionStr, autocommit=True)
    except pyodbc.Error as err:
        logging.warning("Database connection error. " + str(err))
        sys.exit()
    return connection

###SQL Cursor Set Up###
conn = sql_connect()
cursor = conn.cursor()

def todays_date():
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    return today

def cds_list():

    query ='''select distinct cl.bbg_cds_ticker, bd.ticker, bd.issuer from sha2.new_cds_mgmt.cds_list cl left join sha2.new_bond_mgmt.bond_details bd on bd.isin = cl.refob'''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    return df

def historic_ratings_by_ticker_seniority(start_date, end_date, data_set):

    ticker_list = data_set['ticker'].unique()

    query = f'''select b.*,map_rating.Rating as [rating] from 
                (
                select pricedate, 
                ticker as [ticker], 
                Case
                    When seniority like '%sub%' then 'Subordinated'
                    Else seniority
                End as seniority, 
                max([rating_rank]) as [rating_rank]
                from (
                select br.PriceDate, bc.ticker,
                Case 
                    When bc.Perpetual='Y' Then 'Subordinated'
                    When bc.Senior='Y' Then 'Senior'
                    
                    Else bc.Seniority 
                End as [seniority] ,	
                id.rank as [rating_rank]
                
                from 
                sha2.new_bond_mgmt.bond_ratings br
                left join sha2.map.map_rating id on id.ratingID=br.ratingCurrent
                left join sha2.new_bond_mgmt.bond_secmaster bc on bc.BondID=br.BondID
                where pricedate >='{start_date}' and pricedate <='{end_date}' and Seniority in ('Sr Unsecured','Jr Subordinated')
                )a
                group by pricedate, ticker, Seniority
                )b
                left join sha2.map.map_rating as map_rating on map_rating.ratingID=b.rating_rank
                order by pricedate
                '''
    cursor.execute(query)
    cursor.fetchall()
    ratings_df = pd.read_sql(query, conn)

    # Merge data_set with ratings_df
    data_set = data_set.merge(ratings_df, on=['pricedate', 'ticker', 'seniority'], how='left')
    data_set['rating'] = data_set['rating_y']
    # assign original rating method if newer rating is nan
    data_set['rating'] = data_set['rating'].fillna(data_set['rating_x'])
    data_set.drop(['rating_x','rating_y'], axis=1, inplace=True)
    data_set.drop(['rating_rank_x','rating_rank_y'], axis=1, inplace=True)

    # Convert ratings to S&P format
    data_set['rating'] = data_set['rating'].map(variable_keys_beta.rating_key)
    data_set['rating_rank'] = data_set['rating'].map(variable_keys_beta.rating_rank)

    return data_set

def cds_quote_data(start_date,end_date,source='Bloomberg'):
    #runs everything then can filter on it later
    query = f'''select 
                    cq.pricedate as [pricedate], 
                    CASE 
						WHEN cl.murex_name is NULL THEN CONCAT(bd.ticker,' ',cq.tenor) 
						ELSE
							CONCAT(cl.murex_name,' ',cq.tenor) 
					END as [murex_name] ,  
                    cl.seniority as [seniority], 
                    cl.bbg_cds_ticker as [bbg_cds_ticker], 
                    cq.tenor as [tenor],  
                    cq.quote as [quote], 
					cq.[recovery] as [recovery_rate],
                    bd.Sector as [sector], 
                    rating_id.Rating as [rating], 
                    bd.region as [region],
                    bd.ticker as [ticker],
                    bd.country as [country],
					bd.industrygroup as [industrygroup],
				    cl.coupon as [coupon],
					cl.currency as [currency],
					1 as [beta]
				from sha2.new_cds_mgmt.cds_quote cq
                left join sha2.new_cds_mgmt.cds_list cl on cl.bbg_cds_ticker=cq.bbg_cds_ticker
                left join sha2.new_bond_mgmt.bond_details bd on bd.isin=cl.refob
                left join sha2.new_bond_mgmt.bond_ratings br on br.bondid=bd.bondid and br.PriceDate=cq.pricedate
                left join sha2.map.map_rating rating_id on rating_id.ratingID=br.ratingWorst
                where cq.pricedate >='{start_date}' and cq.pricedate <= '{end_date}' 
                and cq.source='Bloomberg' and quote_type='spread'
                order by cq.pricedate'''
    # notes (coupon in bps)
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    #convert rating column to S&P format
    df['rating'] = df['rating'].map(variable_keys_beta.rating_key)
    df['rating_rank'] = df['rating'].map(variable_keys_beta.rating_rank)
    return df

def cds_spread_range_data(end_date,source):
    query = f'''
                    select  distinct    
                    cl.seniority as [seniority],
                    cq.tenor as [tenor],  
					bd.Sector as [sector],
					bd.region as [region],
                    rating_id.Rating as [rating], 
                    avg(cq.quote) as [quote]
           
                from sha2.new_cds_mgmt.cds_quote cq
                left join sha2.new_cds_mgmt.cds_list cl on cl.bbg_cds_ticker=cq.bbg_cds_ticker
                left join sha2.new_bond_mgmt.bond_details bd on bd.isin=cl.refob
                left join sha2.new_bond_mgmt.bond_ratings br on br.bondid=bd.bondid and br.PriceDate=cq.pricedate
                left join sha2.map.map_rating rating_id on rating_id.ratingID=br.ratingWorst
                where cq.pricedate = sha2.dbo.LastWorkday('{end_date}') 
                and cq.source='{source}' and quote_type='spread'
                group by cl.seniority, cq.tenor, bd.Sector, bd.region, rating_id.Rating '''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    # modify ratings. secondary layer. in case ref ob isin wasn't set up historically #Won't slow it down. Current dataframe only
    #back_days_before_end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d') - datetime.timedelta(days=5)
    #df = historic_ratings_by_ticker_seniority(df, start_date=back_days_before_end_date, end_date=end_date)

    # convert rating column to S&P format
    df['rating'] = df['rating'].map(variable_keys_beta.rating_key)
    df['rating_rank'] = df['rating'].map(variable_keys_beta.rating_rank)

    return df

def find_portfolio_bond_data(end_date,bond_portfolio,one_day_ago, one_week_ago, one_month_ago, three_months_ago):
    query = f'''Select fbr.*, bs.Perpetual ,
                CASE WHEN one_day.z_spread is NULL THEN fbr.OAS ELSE one_day.z_spread END as [1D_Bond],
                one_week.z_spread as [1W_Bond], one_month.z_spread as [1M_Bond], three_month.z_spread as [3M_Bond] 
                from sha2.barra.full_barra_risk fbr
                left join sha2.new_bond_mgmt.bond_secmaster bs on bs.isin = fbr.isin
                left join (select isin,z_spread from sha2.new_bond_mgmt.CILibrary_bond_pricing where pricedate = '{one_day_ago}') one_day on one_day.isin=fbr.isin
                left join (select isin,z_spread from sha2.new_bond_mgmt.CILibrary_bond_pricing where pricedate = '{one_week_ago}') one_week on one_week.isin=fbr.isin
                left join (select isin,z_spread from sha2.new_bond_mgmt.CILibrary_bond_pricing where pricedate = '{one_month_ago}') one_month on one_month.isin=fbr.isin
                left join (select isin,z_spread from sha2.new_bond_mgmt.CILibrary_bond_pricing where pricedate = '{three_months_ago}') three_month on three_month.isin=fbr.isin
                where fbr.pricedate = sha2.dbo.LastWorkday('{end_date}') and fbr.portfolio in {bond_portfolio}
                '''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)
    return df

def cds_quote_for_basis(start_date,end_date,source):
    query = f'''select 
                    cq.pricedate as [pricedate], 
                    cl.murex_name [murex_name], 
                    cl.seniority as [seniority], 
                    cl.bbg_cds_ticker as [bbg_cds_ticker], 
					cl.currency as [currency],
                    cq.tenor as [tenor],  
                    cq.quote as [quote], 
                    bd.Sector as [sector], 
                    rating_id.Rating as [rating], 
                    bd.region as [region],
                    bd.ticker as [ticker],
					bd.Country as [country]
                from sha2.new_cds_mgmt.cds_quote cq
                left join sha2.new_cds_mgmt.cds_list cl on cl.bbg_cds_ticker=cq.bbg_cds_ticker
                left join sha2.new_bond_mgmt.bond_details bd on bd.isin=cl.refob
                left join sha2.new_bond_mgmt.bond_ratings br on br.bondid=bd.bondid and br.PriceDate=cq.pricedate
                left join sha2.map.map_rating rating_id on rating_id.ratingID=br.ratingWorst
                where cq.pricedate >='{start_date}' and cq.pricedate <= '{end_date}'
                and cq.source='{source}' and quote_type='spread'
                order by cq.pricedate'''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    # modify ratings. secondary layer. in case ref ob isin wasn't set up historically #WORKS BUT MAKES IT SLOW. IMPROVE WHEN DOING
    #df = historic_ratings_by_ticker_seniority(df, start_date = start_date, end_date = end_date)

    # convert rating column to S&P format
    #df['rating'] = df['rating'].map(variable_keys_beta.rating_key)
    #df['rating_rank'] = df['rating_rank'].map(variable_keys_beta.rating_rank)

    return df

def find_unique_cds_tenor_list(end_date):
    query = f'''select distinct cq.tenor as [cds_tenor] from sha2.new_cds_mgmt.cds_quote cq where cq.pricedate ='{end_date}' '''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)
    return df

def cds_index_tranche_spread_quotes(start_date,end_date,pricing_source):
    if pricing_source =='markit':
        query = f'''Select * from 
                        (select 
                        mts.pricedate as [pricedate],
                        CASE 
                            WHEN mts.markit_index_name = 'iTraxx Eur' THEN 'ITRAXX MAIN'
                            WHEN mts.markit_index_name = 'iTraxx Eur Xover' THEN 'ITRAXX XOVER'
                            WHEN mts.markit_index_name = 'CDXNAIG' THEN 'CDX IG'
                            WHEN mts.markit_index_name = 'CDXNAHY' THEN 'CDX HY'
                            ELSE mts.markit_index_name
                        END as [index_short_name],
                        CASE 
                            WHEN mts.markit_index_name = 'iTraxx Eur' THEN 'IG'
                            WHEN mts.markit_index_name = 'iTraxx Eur Xover' THEN 'HY'
                            WHEN mts.markit_index_name = 'CDXNAIG' THEN 'IG'
                            WHEN mts.markit_index_name = 'CDXNAHY' THEN 'HY'
                            ELSE mts.markit_index_name
                        END as [ig_hy_em],
                        mts.index_series as [index_series],
                        mts.index_coupon * 10000 as [index_coupon],
                        mts.index_version as [index_version],
                        --mts.index_term as [index_tenor],
                        mts.index_maturity as [index_maturity],
                        mts.index_term as [tenor],
                        mts.attachment as [attachment],
                        mts.detachment as [detachment],
                        mts.index_spread_mid as [index_ref_spread_mid],
                        mts.tranche_spread as [spread_quote],
                        'tranche' as [macro_product]
                    
                    from sha2.new_cds_mgmt.[markit_tranche_spreads] mts
                    where mts.pricedate>='{start_date}' and mts.pricedate<='{end_date}' and mts.index_maturity>='{start_date}' --todays date
                    
                    UNION
                    
                    select a.pricedate as [pricedate],
                    CASE 
                            WHEN a.family2 = 'iTraxx Europe' THEN 'ITRAXX MAIN'
                            WHEN a.family2 = 'iTraxx Europe Crossover' THEN 'ITRAXX XOVER'
                            WHEN a.family2 = 'iTraxx Europe Senior Financials' THEN 'ITRAXX FINS SNR'
                            WHEN a.family2 = 'iTraxx Europe Sub Financials' THEN 'ITRAXX FINS SUB'
                            WHEN a.family2 = 'iTraxx Eur Xover' THEN 'ITRAXX XOVER'
                            WHEN a.family2 = 'CDX.NA.IG' THEN 'CDX IG'
                            WHEN a.family2 = 'CDX.NA.HY' THEN 'CDX HY'
                            WHEN a.family2 = 'CDX.EM' THEN 'CDX EM'
                            ELSE a.family2
                        END as [index_short_name],
                        CASE 
                            WHEN a.family2 = 'iTraxx Europe' THEN 'IG'
                            WHEN a.family2 = 'iTraxx Europe Crossover' THEN 'HY'
                            WHEN a.family2 = 'iTraxx Europe Senior Financials' THEN 'IG'
                            WHEN a.family2 = 'iTraxx Europe Sub Financials' THEN 'HY'
                            WHEN a.family2 = 'iTraxx Eur Xover' THEN 'IG'
                            WHEN a.family2 = 'CDX.NA.IG' THEN 'IG'
                            WHEN a.family2 = 'CDX.NA.HY' THEN 'HY'
                            WHEN a.family2 = 'CDX.EM' THEN 'EM'
                            ELSE a.family2
                        END as [ig_hy_em],
                        a.series as [index_series],
                        a.coupon * 10000 as [index_coupon],
                        a.[version] as [index_version],
                        --a.index_term as [index_tenor],
                        a.maturity as [index_maturity],
                        a.term as [tenor],
                        '0' as [attachment],
                        '1' as [detachment],
                        a.quote as [index_ref_spread_mid],
                        a.quote as [spread_quote],
                        'index' as [macro_product]
                    from 
                    (select distinct cdx_spread.pricedate, cdx_spread.quote, cdx_list.series, cdx_list.[version],cdx_list.maturity,cdx_list.term, cdx_list.family2, cdx_list.coupon
                    from sha2.new_cds_mgmt.cdx_spread cdx_spread 
                    inner join sha2.new_cds_mgmt.cdx_list cdx_list on cdx_list.cdx_id=cdx_spread.cdx_id
                    where cdx_spread.pricedate >='{start_date}' and cdx_spread.pricedate <='{end_date}' and cdx_list.maturity >='{start_date}'  
                    and quote_type='PX_MID 15:00' and [version] <>0 and cdx_list.family <>'BESPOKE') as a
                    ) as b
				where b.index_series >=(select max(series) from sha2.new_cds_mgmt.cdx_list)-20
                '''
    elif pricing_source =='bloomberg':
        pass
        #fill query command from sql
    else:
        print('error')
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    # add years to maturity column, currency column
    df['index_maturity'] = pd.to_datetime(df['index_maturity'])
    df['index_currency'] = df['index_short_name'].map(variable_keys_beta.cds_index_currency_key)

    # add region column
    df['index_region'] = df['index_short_name'].map(variable_keys_beta.cds_index_region_exposure)

    # removed duplicate lines for version x, y as we care about the latest version. Plus index quotes/ref quotes are the same
    df = df.sort_values(by='index_version', ascending=False)
    df = df.drop_duplicates(subset=['pricedate','index_short_name', 'index_series', 'index_maturity', 'tenor', 'attachment', 'detachment','macro_product'])
    df = df.sort_values(by='pricedate', ascending=True)

    # update index versions for 3Y, 7Y, 10Y as only the 5Y seems gets updated. Find max for date, and set all tenors the same
    max_series_df = df.loc[df.groupby(['pricedate', 'index_short_name', 'index_series'])['index_version'].idxmax().reset_index(drop=True)]
    df = df.merge(max_series_df, on=['pricedate', 'index_short_name', 'index_series'], suffixes=('', '_max'), how='left')
    df['index_version'] = df['index_version_max']
    df.drop(columns=[col for col in df.columns if col.endswith('_max')], inplace=True)


    return df

def cds_index_tranche_spread_cds_constituents(start_date=None, end_date=None, pricing_source=None):
    # the constituents show the most recent version and only displays the 5yr maturity date. any defaults here have a weight of zero
    query = f'''select
                CASE 
                        WHEN a.family2 = 'iTraxx Europe' THEN 'ITRAXX MAIN'
                        WHEN a.family2 = 'iTraxx Europe Crossover' THEN 'ITRAXX XOVER'
                        WHEN a.family2 = 'iTraxx Europe Senior Financials' THEN 'ITRAXX FINS SNR'
                        WHEN a.family2 = 'iTraxx Europe Sub Financials' THEN 'ITRAXX FINS SUB'
                        WHEN a.family2 = 'iTraxx Eur Xover' THEN 'ITRAXX XOVER'
                        WHEN a.family2 = 'CDX.NA.IG' THEN 'CDX IG'
                        WHEN a.family2 = 'CDX.NA.HY' THEN 'CDX HY'
                        WHEN a.family2 = 'CDX.EM' THEN 'CDX EM'
                        ELSE a.family2
                    END as [index_short_name],
                    CASE 
                        WHEN a.family2 = 'iTraxx Europe' THEN 'IG'
                        WHEN a.family2 = 'iTraxx Europe Crossover' THEN 'HY'
                        WHEN a.family2 = 'iTraxx Europe Senior Financials' THEN 'IG'
                        WHEN a.family2 = 'iTraxx Europe Sub Financials' THEN 'HY'
                        WHEN a.family2 = 'iTraxx Eur Xover' THEN 'IG'
                        WHEN a.family2 = 'CDX.NA.IG' THEN 'IG'
                        WHEN a.family2 = 'CDX.NA.HY' THEN 'HY'
                        WHEN a.family2 = 'CDX.EM' THEN 'EM'
                        ELSE a.family2
                    END as [ig_hy_em],
                    a.series as [index_series],
                    a.[version] as [index_version],
                    --a.index_term as [index_tenor],
                    a.maturity as [index_maturity],
                    a.bbg_cds_ticker as [cds_constituents],
                    a.index_weight/100 as [cds_weight]
                from 
                (select distinct cdx_list.series, cdx_list.[version],cdx_list.maturity, cdx_list.family2, cdx_underlyings.bbg_cds_ticker, cdx_underlyings.index_weight
                from sha2.new_cds_mgmt.cdx_list cdx_list
                inner join sha2.new_cds_mgmt.cdx_underlyings cdx_underlyings on cdx_list.cdx_id=cdx_underlyings.cdx_id
                where  [version] <>0 and cdx_list.family<>'BESPOKE') as a '''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    # reformat
    df['cds_weight'] = pd.to_numeric(df['cds_weight'], errors='coerce')
    df['index_maturity'] = pd.to_datetime(df['index_maturity'])
    df = df[(df['index_maturity'] >= end_date)]

    for index, row in df.iterrows():
        index_maturity_years = (((pd.to_datetime(row['index_maturity']) - datetime.datetime.now()).days) / 365.25)
        tenor = min(variable_keys_beta.tenor_to_year.keys(),
                    key=lambda x: abs(variable_keys_beta.tenor_to_year[x] - index_maturity_years))
        df.loc[index, 'tenor'] = tenor
        df.loc[index, 'index_maturity_years'] = index_maturity_years

    return df


def cds_defaults():
    query = f'''select bbg_cds_ticker , default_date, recovery_rate from sha2.new_cds_mgmt.cds_defaults '''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)
    return df

    #https://www.creditfixings.com/CreditEventAuctions/AuctionByYear.jsp?year=2020

def interest_rates_swap_curves(end_date):
    x = 0
    df = pd.DataFrame
    while df.empty:  # finds latest interest rates swap curve
        latest_date_check = datetime.datetime.strptime(end_date,'%Y-%m-%d') - datetime.timedelta(days=x)
        query = f'''select  
                        Currency as [currency],
                        rtshType as [rate_type],
                        Generator as [generator], 
                        Maturity as [tenor], 
                        quote as [quote]
                        from sha2.rates.asset_list 
                        inner join sha2.rates.asset_quotes on asset_list.asset_id = asset_quotes.asset_id 
                        inner join sha2.rates.murex_ir_map mim on mim.bloomberg_ticker = asset_list.bloomberg_ticker
                        where pricedate = '{latest_date_check}' and mx_load = 1 and RtshType like 'SWAP' 
                        and (Generator like '%_USD SOFR A%' or Generator like '%EURIBOR A 3M%' or Generator like '%GBP SONIA S 6M LCH%')
                        order by currency, Maturity '''
        cursor.execute(query)
        cursor.fetchall()
        df = pd.read_sql(query, conn)
        df['tenor_years'] = df['tenor'].map(lambda x: int(''.join(filter(str.isdigit, x))))
        for i in range(len(df['tenor_years'])):
            if df.loc[i, 'tenor'].endswith('W'):
                df.loc[i, 'tenor_years'] = df.loc[i, 'tenor_years'] / 52
            elif df.loc[i, 'tenor'].endswith('M'):
                df.loc[i, 'tenor_years'] = df.loc[i, 'tenor_years'] / 12
            elif df.loc[i, 'tenor'].endswith('Y'):
                df.loc[i, 'tenor_years'] = df.loc[i, 'tenor_years']
            else:
                continue
        df = df.sort_values(by=['tenor_years'], ascending=True)
        x += 1

    return df

def filter_cds(df,sector,industry_group,seniority,region,cds_1_tenor,cds_2_tenor, ig_hy):
    # - filtering and cleaning - sectors, seniority, date_exclusion_set ###

    # date inclusion ranges (dont need if putting in beta event by date overrides
    #mask1 = (df['pricedate'] >= '2017-01-01') & (df['pricedate'] <= '2020-02-10')
    #mask2 = (df['pricedate'] >= '2020-08-01') & (df['pricedate'] <= '2021-12-31')
    #mask3 = (df['pricedate'] >= '2023-12-01')

    #df = df.loc[(mask1 & mask2 & mask3)]

    # filter for specific cds
    #df = df[df['bbg_cds_ticker'].isin(['CACC1E5','CT354143'])]
    #if ticker is not None:
        #df = df[df['ticker'].isin(ticker)]

    if 'All' not in sector:
        df = df[df['sector'].isin(sector)]

    if 'All' not in industry_group:
        df = df[df['industrygroup'].isin(industry_group)]
    df = df.drop('industrygroup', axis=1)

    df = df[df['seniority'].isin(seniority)]
    df = df[df['region'].isin(region)]
    df = df[df['tenor'].isin([cds_1_tenor, cds_2_tenor])]
    #add column for beta, default to 1 for all functions to operate
    df['beta'] = 1

    # clean rating
    df['rating'] = df['rating'].map(variable_keys_beta.rating_key)
    df = df[df['rating'].notna()]
    df['rating_rank_temp'] = df['rating']
    df['rating_rank_temp'] = df['rating_rank_temp'].map(variable_keys_beta.rating_rank)
    if (ig_hy == 'IG') or (ig_hy == 'ig'):
        df = df[df['rating_rank_temp'] <= 10]
    elif (ig_hy == 'HY') or (ig_hy == 'hy'):
        df = df[df['rating_rank_temp'] >= 11]
    else:
        pass
    df = df.drop('rating_rank_temp', axis=1)

    return df

class generate_dataframes:

    @staticmethod
    def filter_results_pre(df, equity_tranche_included, maturity_range, cds_series_inclusion, end_date):

        # filter matured products
        df =df[df['index_maturity_years'] >= 0]

        # filter pricedates column
        df = df[df['pricedate'] <= end_date]
        #price_date_list = df['pricedate'].unique()

        # filter for equity tranches
        if equity_tranche_included == 'N':
            df = df[~((df['attachment'] == 0) & (df['detachment'] != 1))]

        # filter for maturity_range
        lower_range_maturity, upper_range_maturity = maturity_range[0], maturity_range[1]
        df = df[(df['index_maturity_years'] >= lower_range_maturity) & (df['index_maturity_years'] <= upper_range_maturity)]

        # filter to include latest tradeable series
        max_series = df['index_series'].max()
        df = df[df['index_series'] >= (max_series+cds_series_inclusion)]

        ### REQUIRED ONLY FOR THE HISTORICAL REGRESION PART FOR FINDING PAIR TRADES
        # beta_data_filter
        #if beta_data_filter in ['Y', 'y', 'yes','Yes']:
        #    df = beta_adjustments.beta_overrides_by_date(df,method='index')


        df['index_maturity_years'].round(1)

        return df

    def index_tranche_generic_key_df(self,df):
        df = df[(df['attachment'] == 0) & (df['detachment'] == 1)]
        df = df[['pricedate', 'index_short_name', 'index_series', 'index_version']]
        return df

    def index_tranche_latest_versions_series_generic_key_df(self, df):
        index_tranche_generic_key_list = []
        for pricedate in df['pricedate'].unique():
            unique_index_df = df[df['pricedate'] == pricedate]
            index_short_name_list = unique_index_df['index_short_name'].unique()
            for index_short_name in index_short_name_list:
                unique_index_df_v2 = unique_index_df[unique_index_df['index_short_name'] == index_short_name]
                unique_index_df_v2 = unique_index_df_v2[unique_index_df_v2['index_series'] == unique_index_df_v2['index_series'].max()]
                unique_index_df_v2 = unique_index_df_v2[unique_index_df_v2['index_version'] == unique_index_df_v2['index_version'].max()]
                try:
                    index_tranche_generic_key_list.append([unique_index_df_v2['pricedate'].values[0], unique_index_df_v2['index_short_name'].values[0], unique_index_df_v2['index_series'].values[0],unique_index_df_v2['index_version'].values[0]])
                except:
                    continue
        index_tranche_generic_key_df = pd.DataFrame(list(index_tranche_generic_key_list),columns=['pricedate', 'index_short_name', 'index_series','index_version'])

        return index_tranche_generic_key_df

    def index_tranche_latest_versions_generic_key_df(self,df,):
        index_tranche_latest_versions_list = []
        for pricedate in df['pricedate'].unique():
            unique_index_df = df[df['pricedate'] == pricedate]
            index_short_name_series_list = unique_index_df[['index_short_name','index_series']].drop_duplicates()
            for index, row in index_short_name_series_list.iterrows():
                unique_index_df_v2= unique_index_df[(unique_index_df['index_short_name'] == row['index_short_name']) & (unique_index_df['index_series'] == row['index_series'])]
                unique_index_df_v2 = unique_index_df_v2[unique_index_df_v2['index_version'] == unique_index_df_v2['index_version'].max()]
                try:
                    index_tranche_latest_versions_list.append([unique_index_df_v2['pricedate'].values[0], unique_index_df_v2['index_short_name'].values[0], unique_index_df_v2['index_series'].values[0],unique_index_df_v2['index_version'].values[0]])
                except:
                    continue
        index_tranche_latest_versions_generic_key_df = pd.DataFrame(list(index_tranche_latest_versions_list),columns=['pricedate', 'index_short_name', 'index_series', 'index_version'])
        return index_tranche_latest_versions_generic_key_df
