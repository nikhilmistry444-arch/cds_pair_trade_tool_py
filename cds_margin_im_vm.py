#!/usr/bin/env python3
# cds_margin_im_vm.py
# -------------------
# Single-leg margin calculator for CDS indices using *total* CR01 (already FX-adjusted and notional-scaled).
# Keeps full Variation Margin (VM) logic per leg so notionals and risk per leg are preserved.
#
# Computes for each pricedate:
# - VM_EUR  = direction * |cr01| * dspread_bp
# - IM_EUR  = |cr01| * RW_bp * sqrt(mpor_days / base_mpor_days)
# - Daily_Margin_Requirement = IM + |cumulative VM|
#
# Why only single-leg (no correlations here)?
# - You're running one leg at a time, then offsetting legs later by trade direction in your own process.
# - Correlations only matter when aggregating *multiple* legs/buckets into a portfolio IM.
#
# What are mpor_days and base_mpor_days?
# - mpor_days (Margin Period of Risk): assumed liquidation/close-out horizon in days.MPOR is the assumed number of days this takes, during which you’re at risk
#   Typical industry value ~10 business days.
# - base_mpor_days: the reference horizon used for calibrating risk weights (often 10 days in SIMM).
#   We scale risk as sqrt(mpor_days / base_mpor_days) so if you change MPOR, IM scales appropriately.

from typing import Dict, Optional
import math
import pandas as pd
import variable_keys_beta
import tranche_delta_runs


# -----------------------------
# Core single-leg formulas
# -----------------------------
def default_risk_weights_bp(bucket, traded_spread, delta):
    # take discount off from traded spread as E(l) (expected loss). Can make function of companies stability. Company specific
    discount_dict = {'IG_EUR': 0.75, 'HY_EUR': 0.5, 'IG_USD': 0.75, 'HY_USD': 0.5, 'EM_USD': 0.5, 'EM_EUR': 0.5}
    if isinstance(bucket, pd.Series):
        discount_multiplier = discount_dict.get(bucket)
    else:
        discount_multiplier = discount_dict[bucket]

    raw_bp = traded_spread * (1-discount_multiplier) * delta

    return raw_bp
def estimate_im_single_leg(cr01: float,
                           bucket: str,
                           traded_spread: float,
                           delta: float,
                           b_s_protection: str,
                           mpor_days: int = 10,
                           base_mpor_days: int = 10) -> float:
    """
    Simplified Initial Margin Model (SIMM) for a single leg (no correlations):
      IM = |CS01_total| * RW_bp * sqrt(MPOR / base_MPOR)
    - |CS01_total| is the magnitude of total CR01 for that leg (already in EUR/bp).
    - RW_bp is the risk weight (bp) for the leg's bucket (e.g., IG_EUR, HY_USD). HY is 80 as 80 over 250 is smaller percentage move. based on SIMM historical stress scenarios, historical vol and relative riskiness. It does not equal current market spreads
    - Direction does not affect IM magnitude.
    """
    rw_bp = default_risk_weights_bp(bucket, traded_spread, delta)
    if rw_bp is None:
        raise ValueError(f"No risk weight for bucket {bucket!r}")
    scale = math.sqrt(max(mpor_days, 1) / max(base_mpor_days, 1))

    margin_direction = 1 if b_s_protection == 'Sell Protection' else -1

    return abs(cr01) * rw_bp * scale * margin_direction

def _infer_bucket(index_short_name: str, index_currency: str, ig_hy_em: str) -> str:
    idx = (index_short_name or '').upper()
    cur = (index_currency or '').upper()
    grade = (ig_hy_em or '').upper()
    if 'HY' in idx or grade == 'HY':
        return 'HY_USD' if cur == 'USD' else 'HY_EUR'
    elif 'EM' in idx or grade =='EM':
        return 'EM_USD' if cur == 'USD' else 'EM_EUR'
    else:
        return 'IG_USD' if cur == 'USD' else 'IG_EUR'



def compute_daily_vm_im_margin_macro(
    df, #table to apply daily margin calculations
    traded_spread, # spread in bps
    b_s_protection, # 'Sell Protection' or 'Buy Protection'
    process: str='time_series', # as opposed to product attributes
    mpor_days: int = 10,
    base_mpor_days: int = 10,
):
    """
    Compute daily VM/IM for a single CDS leg timeseries (cr01 = total /bp, signed).
    Returns the original DataFrame with added columns:
      - direction
      - bucket
      - VM_EUR
      - IM_EUR
      - Daily_Margin_Requirement
    """
    work = df.copy()

    # Daily spread change
    # for i in range(len(df)):
    #     if i == 0:
    #         work.loc[i, 'DoD_spread_quote_chg'] = traded_spread - work.loc[i, 'spread_quote']
    #     else:
    #         work.loc[i, 'DoD_spread_quote_chg'] = work.loc[i, 'spread_quote'] - work.loc[i-1, 'spread_quote']

    # Map to a bucket for risk weight (used only for IM)
    if isinstance(work, pd.Series):
        work['bucket'] = _infer_bucket(
                work['index_short_name'],
                work['index_currency'],
                work['ig_hy_em'],
            )
        macro_product = work['macro_product']
        index_short_name = work['index_short_name']
        index_series = work['index_series']
        attachment = work['attachment']
        detachment = work['detachment']

    else:
        work['bucket'] = [
            _infer_bucket(
                work.at[i, 'index_short_name'] if 'index_short_name' in work.columns else None,
                work.at[i, 'index_currency'] if 'index_currency' in work.columns else None,
                work.at[i, 'ig_hy_em'] if 'ig_hy_em' in work.columns else None,
            )
            for i in work.index
        ]
        macro_product = work['macro_product'].values[0]
        index_short_name = work['index_short_name'].values[0]
        index_series = work['index_series'].values[0]
        attachment = work['attachment'].values[0]
        detachment = work['detachment'].values[0]



    if macro_product == 'index':
        delta = 1
    else:
        tranche_delta_df = tranche_delta_runs.tranche_deltas()
        dictionary_finder = str(index_short_name.replace(' ', '_')) + str('_') + str(5)
        bbg_cds_ticker = str(variable_keys_beta.cds_index_bbg_core_ticker[index_short_name]) + str(5) + str(index_series)
        attachment_detachment = str(int(attachment * 100)) + str('-') + str(int(detachment * 100))
        try:
            tranche_delta = tranche_delta_df[(tranche_delta_df['index_short_name_generic'] == dictionary_finder) & (tranche_delta_df['bbg_index_number'] == bbg_cds_ticker) & (tranche_delta_df['attachment-detachment'] == attachment_detachment)]['delta'].values[0]
        except:
            tranche_delta = tranche_delta_runs.backup_tranche_deltas(df=tranche_delta_df,
                                                                     dictionary_finder=dictionary_finder,
                                                                     attachment_detachment=attachment_detachment,
                                                                     current_series=index_series)
        delta=tranche_delta

    # Row-by-row VM and IM
    vms, ims = [], []

    if process == 'time_series':
        work['VM'] = (work['cash_usage_carry'] + work['rolldown_carry'] + work['default_loss']) * -1 # need to be negative to reduce margin if positive pnl
    else: # product by product
        work['VM'] = 0

    if process == 'time_series':
        work['IM'] = work.apply(lambda row: estimate_im_single_leg(cr01=row['cr01'], bucket=row['bucket'], traded_spread=traded_spread, delta=delta, b_s_protection=b_s_protection, mpor_days=mpor_days, base_mpor_days=base_mpor_days), axis=1)
    else: # product by product
        work['IM'] = estimate_im_single_leg(cr01=work['index_maturity_years'], bucket=work['bucket'], traded_spread=traded_spread, delta=delta, b_s_protection=b_s_protection, mpor_days=mpor_days, base_mpor_days=base_mpor_days)

    # Daily margin requirement = IM + |cumulative VM since inception|
    if isinstance(work, pd.Series):
        work['margin'] = work['IM'] + work['VM']
        work['margin'] = 0 if  work['margin']<0 else work['margin']
    else:
        work['margin'] = work['IM'] + work['VM']
        work['margin'] = work['margin'].clip(lower=0)

    return work
