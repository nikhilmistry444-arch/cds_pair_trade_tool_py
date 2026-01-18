import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import sys
import pickle
import os
import pyodbc

from calendar import isleap

sys.path.insert(0,'C:\\Users\\nmistry\OneDrive - Canada Pension Plan Investment Board\Documents\CPPIB\python_notebook\cds_pair_trades')
import datetime
from dateutil.relativedelta import relativedelta
import variable_keys_beta


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

def find_annualised_pnl():
    # find Annualized PnL and Sharpe Ratio:
    # Load your data
    file_path = os.path.join(os.getcwd(),'results_excel', 'back_test', 'file_sets','2024-2025, Optimum-Carry above -25bps, Percent Return above 10%.xlsx')
    df = pd.read_excel(file_path, sheet_name='daily_pnl_pairs',usecols='A:E')
    df['pricedate'] = pd.to_datetime(df['pricedate'])

    # Extract year and month
    df["year"] = df["pricedate"].dt.year
    df["month"] = df["pricedate"].dt.month

    # Step 1: Count unique months per year
    unique_months_per_year = df.groupby("year")["month"].nunique()
    unique_months_df = unique_months_per_year.to_frame(name="unique_months")

    # Step 2: Get year-end cumulative PnL and upfront capital
    year_end = df.groupby("year").agg({
        "rolling_pnl": "last",
        "cash usage": "last"
    })

    # Step 3: Merge the two
    merged_df = year_end.merge(unique_months_df, left_index=True, right_index=True)
    merged_df = merged_df.reset_index()


    # Step 4: Calculate Data
    merged_df['Annual PnL'] = 0
    for i in merged_df.index:
        try:
            merged_df.loc[i,'Annual PnL'] = merged_df.loc[i,'rolling_pnl'] - merged_df.loc[i-1,'rolling_pnl']
        except:
            merged_df['Annual PnL']= merged_df.loc[i,'rolling_pnl']

    merged_df['Annual PnL'] = merged_df['Annual PnL'] * (12/merged_df['unique_months'])

    merged_df['Avg Annual PnL']=merged_df['Annual PnL'].mean()
    #merged_df['Std Annual PnL']=merged_df['Annual PnL'].std()

    for i in merged_df.index:
        if merged_df.loc[i,'cash usage']<0 and merged_df.loc[0,'rolling_pnl']>0:
            merged_df.loc[i,'Annual Return']=100
        elif merged_df.loc[i,'cash usage']<0 and merged_df.loc[0,'rolling_pnl']<0:
            merged_df.loc[i,'Annual Return']=-100
        else:
            merged_df.loc[i,'Annual Return']=merged_df.loc[i,'Annual PnL'] / merged_df.loc[i,'cash usage']

    merged_df['Avg Annual Return']=merged_df['Annual Return'].mean()

    #risk_free_rate = 0.04
    #merged_df['sharpe ratio'] = (merged_df['Annual Return Log'])-risk_free_rate
    #merged_df['sharpe ratio'] = merged_df['sharpe ratio'].mean()/merged_df['sharpe ratio'].std()

    initial_value=1
    final_value= merged_df.loc[merged_df['year'].idxmax(),'rolling_pnl']/1000000
    years = merged_df['unique_months'].sum()/12
    log_growth_rate = np.log(final_value / initial_value) / years
    merged_df['log growth rate'] = log_growth_rate

    print(merged_df)
    print(merged_df)

def find_portfolio_notional_default():
    # Load your data
    file_path = os.path.join(os.getcwd(),'results_excel', 'back_test', 'file_sets','2021-2025, Positive Carry Trades.xlsx')
    df = pd.read_excel(file_path, sheet_name='index_tranche_pair_trades',usecols='B:R')
    df1 = df[['Index-1','Att-Detach 1','Trade 1','Notional 1']]
    df2 = df[['Index-2', 'Att-Detach 2', 'Trade 2', 'Notional 2']]

    df1 = df1[df1['Att-Detach 1'] == '0.0-1.0']
    df2 = df2[df2['Att-Detach 2'] == '0.0-1.0']

    df1['Notional 1'] = df1.apply(lambda row: row['Notional 1'] * -1 if row['Trade 1'] == 'Sell Protection' else row['Notional 1'], axis=1)
    df2['Notional 2'] = df2.apply(lambda row: row['Notional 2'] * -1 if row['Trade 2'] == 'Sell Protection' else row['Notional 2'], axis=1)

    default_multiplier = {
        'CDX IG': (3/125) * 0.6,
        'CDX HY': (5/100) * 0.8,
        'ITRAXX MAIN': (2/125) * 0.6,
        'ITRAXX XOVER': (5/75) * 0.8,
        'ITRAXX FINS SUB': (2/30) * 0.6,
        'ITRAXX FINS SNR': (2/30) * 0.8,
    }

    df1['default_multipler'] = df1['Index-1'].map(default_multiplier)
    df2['default_multipler'] = df2['Index-2'].map(default_multiplier)

    df1['default loss'] = df1['default_multipler'] * df1['Notional 1']
    df2['default loss'] = df2['default_multipler'] * df2['Notional 2']

    df1 = df1.rename(columns={'Index-1': 'index'})
    df2 = df2.rename(columns={'Index-2': 'index'})

    df1 = df1[['index','default loss']]
    df2 = df2[['index','default loss']]

    combined_df = pd.concat([df1,df2], ignore_index=True)

    final_df = combined_df.groupby('index').sum().reset_index()

    print((sum(final_df['default loss'])/1000000)/5)
    print(final_df)
    print(final_df)

find_annualised_pnl()
#find_portfolio_notional_default()

def cds_list():

    query ='''select distinct cl.bbg_cds_ticker, bd.ticker, bd.issuer from sha2.new_cds_mgmt.cds_list cl left join sha2.new_bond_mgmt.bond_details bd on bd.isin = cl.refob'''
    cursor.execute(query)
    cursor.fetchall()
    df = pd.read_sql(query, conn)

    return df
