
import cds_raw_data
import cds_momentum_signal
import cds_calculator
import variable_keys_beta
import transaction_cost_calculator
import spread_ranges_generator
import tranche_delta_runs
import cds_raw_data_cache
import live_data

import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import sys
import pickle
import os
import pyodbc

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
from scipy import stats

from xbbg import blp

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

def update_cds_ref_ob_isins():
    # table -> sha2.new_cds_mgmt.cds_list

    # MISSING RATINGS FOR LIVE ISINS. TO CHECK IN SQL
    '''
        Select * from sha2.new_bond_mgmt.bond_details where isin in
        (
        select cl.refob
        from sha2.new_cds_mgmt.cds_list cl
        inner join sha2.new_bond_mgmt.bond_secmaster bd on bd.isin=cl.refob
        inner join sha2.[new_bond_mgmt].[bond_rating_view] rt on rt.bondid=bd.bondid
        inner join (SELECT distinct bbg_cds_ticker FROM sha2.new_cds_mgmt.cds_quote WHERE PRICEDATE=sha2.dbo.LastWorkday(CURRENT_TIMESTAMP) and source='markit' and tenor='5Y') cq on cq.bbg_cds_ticker=cl.bbg_cds_ticker
        where rt.PriceDate=sha2.dbo.LastWorkday(CURRENT_TIMESTAMP) and rt.worst is null
        )
    '''


    # find matured ISINs/refob like '%REFOB%' only if we have quotes for those
        # expect others in cds list with null ref obs are outdated
    query_data_to_run = '''SELECT a.* FROM 
                                (SELECT cl.*, bd.isin, bd.ticker as [ticker],
                                           CASE 
                                                WHEN 
                                                    (CASE 
                                                    WHEN COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') 
                                                        AND COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.FinalMaturity
                                                    WHEN COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') 
                                                        AND COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.nextCallDate
                                                    ELSE workout.maturity
                                                    END) IS NULL THEN '' 
                                            ELSE  (CASE 
                                                    WHEN COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') 
                                                        AND COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.FinalMaturity
                                                    WHEN COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') 
                                                        AND COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.nextCallDate
                                                    ELSE workout.maturity
                                                    END)
                                            END AS [maturity]
                                    FROM sha2.new_cds_mgmt.cds_list cl
                                    LEFT JOIN sha2.new_bond_mgmt.bond_secmaster bd ON cl.refob = bd.ISIN
                                    LEFT JOIN (SELECT isin, MAX(workout_date) AS [maturity] 
                                                FROM sha2.new_bond_mgmt.bond_analytics_workoutdt_new 
                                                GROUP BY isin) workout ON workout.isin = bd.isin) a
                                
                                INNER JOIN (SELECT distinct bbg_cds_ticker FROM sha2.new_cds_mgmt.cds_quote WHERE PRICEDATE=sha2.dbo.LastWorkday(CURRENT_TIMESTAMP) and source='markit' and tenor='5Y') cds_quote  on a.bbg_cds_ticker=cds_quote.bbg_cds_ticker
                                WHERE a.[maturity]<=FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd') or (a.refob like '%ref%') or (a.refob like '%MISSING%') or (a.refob is null) or (a.refob ='nan')
                                 '''
    cursor.execute(query_data_to_run)
    cursor.fetchall()
    df_data_to_run = pd.read_sql(query_data_to_run, conn)


    # find ref obs (
    for index, row in df_data_to_run.iterrows():
        missing_ref_ob = 'MISSING'
        bbg_cds_ticker = str(row['bbg_cds_ticker']) + str(' CBIN Curncy')
        # find ref obs
        ref_ob = blp.bdp(bbg_cds_ticker,'SW_PRIMARY_REF_OBLIGATION_ISIN').iloc[0, 0]
        if (pd.isna(ref_ob) == True) or ('REF' in ref_ob):
            ref_ob = blp.bdp(bbg_cds_ticker, 'REAL_PRIMARY_RO_ISIN').iloc[0, 0]
            if (pd.isna(ref_ob) == True) or ('REF' in ref_ob):
                ref_ob = missing_ref_ob
        # check maturity
        if ref_ob != missing_ref_ob:
            bbg_isin_ticker = str(ref_ob) + str(' Corp')
            try:
                maturity_of_ref = blp.bdp(bbg_isin_ticker,'YAS_WORKOUT_DT').iloc[0,0]
                if pd.isna(maturity_of_ref) == True:
                    ref_ob = missing_ref_ob
                elif maturity_of_ref <= datetime.datetime.now().strftime('%Y-%m-%d'):
                    ref_ob = missing_ref_ob
                else:
                    pass
            except:
                # error as bond has matured
                ref_ob = missing_ref_ob

        # assign cppib ref ob from isins from bond_details for missing_ref_ob
        if ref_ob == missing_ref_ob:
            collateral_key_to_bonds = {
                'Senior': ['Secured','1st lien','Sr Preferred','Sr Unsecured'],
                'SLA': ['Sr Non Preferred'],
                'Subordinated': ['Sr Subordinated','Subordinated','Jr Subordinated']
            }

            ccy = row['currency']
            seniority_pc = str(collateral_key_to_bonds[row['seniority']]).strip('[]')
            ticker = row['ticker'] #based off of old ref ob mapped to bond_details
            if (ticker is None) or (ticker=='') or (ticker=='None'):
                bbg_cds_ticker = str(row['bbg_cds_ticker']) + str(' CBIN Curncy')
                ticker = blp.bdp(bbg_cds_ticker, 'CDS_CORP_TKR').iloc[0, 0]

            try: # try find isin from bond_details that valid using ticker from the old ref ob
                query_find_potential_ref_ob = f'''SELECT TOP 1 * FROM (
                                                    SELECT bd.isin,
                                                           CASE 
                                                               WHEN COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') 
                                                                    AND COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.FinalMaturity
                                                               WHEN COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') 
                                                                    AND COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.nextCallDate
                                                               ELSE workout.maturity
                                                           END AS [maturity]
                                                    FROM sha2.new_bond_mgmt.bond_secmaster bd 
                                                    LEFT JOIN sha2.map.map_currency ccy on ccy.currencyID = bd.CurrencyID
                                                    LEFT JOIN (SELECT isin, MAX(workout_date) AS [maturity] 
                                                               FROM sha2.new_bond_mgmt.bond_analytics_workoutdt_new 
                                                               GROUP BY isin) workout ON workout.isin = bd.isin
                                                    WHERE bd.ticker = '{ticker}' 
                                                      AND ccy.currencySymbol = '{ccy}' 
                                                      AND seniority IN ({seniority_pc})
                                                      ) a
                                                    WHERE (a.[maturity]>FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd') and a.[maturity] is not null)  
                                                    ORDER BY a.maturity DESC'''
                cursor.execute(query_find_potential_ref_ob)
                cursor.fetchall()
                potential_ref_ob_df = pd.read_sql(query_find_potential_ref_ob, conn)
                ref_ob = potential_ref_ob_df['isin'].values[0]

            except: # run bbg_cds_ticker to find recent corp_ticker to see if theres a valid isin in bond_details
                try:
                    bbg_cds_ticker = str(row['bbg_cds_ticker']) + str(' CBIN Curncy')
                    ticker = blp.bdp(bbg_cds_ticker, 'CDS_CORP_TKR').iloc[0, 0]
                    query_find_potential_ref_ob = f'''SELECT TOP 1 * FROM (
                                                        SELECT bd.isin,
                                                           CASE 
                                                               WHEN COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') 
                                                                    AND COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.FinalMaturity
                                                               WHEN COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, bd.FinalMaturity), '1900-01-01') 
                                                                    AND COALESCE(CONVERT(DATE, bd.nextCallDate), '1900-01-01') >= COALESCE(CONVERT(DATE, workout.maturity), '1900-01-01') THEN bd.nextCallDate
                                                               ELSE workout.maturity
                                                           END AS [maturity]
                                                    FROM sha2.new_bond_mgmt.bond_secmaster bd 
                                                    LEFT JOIN sha2.map.map_currency ccy on ccy.currencyID = bd.CurrencyID
                                                    LEFT JOIN (SELECT isin, MAX(workout_date) AS [maturity] 
                                                               FROM sha2.new_bond_mgmt.bond_analytics_workoutdt_new 
                                                               GROUP BY isin) workout ON workout.isin = bd.isin
                                                    WHERE bd.ticker = '{ticker}' 
                                                      AND ccy.currencySymbol = '{ccy}' 
                                                      AND seniority IN ({seniority_pc})
                                                      ) a
                                                    WHERE (a.[maturity]>FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd') and a.[maturity] is not null)  
                                                    ORDER BY a.maturity DESC'''
                    cursor.execute(query_find_potential_ref_ob)
                    cursor.fetchall()
                    potential_ref_ob_df = pd.read_sql(query_find_potential_ref_ob, conn)
                    ref_ob = potential_ref_ob_df['isin'].values[0]
                except: # not even the secondary ticker is valid for bonds in database so assign 'MISSING' to the ref ob
                    ref_ob = missing_ref_ob

        # find an alternative ref ob from the database that matches similar criteria
        if ref_ob != row['refob']:
            update_statement =f''' update sha2.new_cds_mgmt.cds_list set refob='{ref_ob}' where bbg_cds_ticker='{row['bbg_cds_ticker']}' '''
            cursor.execute(update_statement) # execute the update statement
            conn.commit() # commit the transaction


#area to run functions
update_cds_ref_ob_isins()