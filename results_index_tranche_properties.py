import pandas as pd


class end_results:

    def __init__(self, df):
        df['spread_quote'] = df['spread_quote'].round(1)
        df['index_ref_spread_mid'] = df['index_ref_spread_mid'].round(1)
        df['index_maturity_years'] = df['index_maturity_years'].round(1)
        df['index_maturity'] = pd.to_datetime(df['index_maturity']).dt.date
        df['net_carry_to_maturity_bps'] = df['net_carry_to_maturity_bps'].round(0)
        df['net_carry_to_maturity_default_bps'] = df['net_carry_to_maturity_default_bps'].round(0)
        #drop columns
        df.drop(['index_coupon','index_currency'], axis=1, inplace=True)
        df.drop(['DoD_ref_spread_chg'], axis=1, inplace=True, errors='ignore')

        self.df = df



    def simple_end_results(self):
        simplified_cols = [
            'pricedate',
            'index_short_name',
            'index_series',
            'tenor',
            'index_maturity',
            'index_maturity_years',
            'attachment',
            'detachment',
            'index_ref_spread_mid',
            'bid',
            'liquid_notional',
            'spread_ranges',
            'upfront_bps',
            'margin_bps',
            'momentum_spread',
            'basis',
            '12mR+C-Same Series',
            'RC % Rtn',
            'RC SIGNAL',
            'carry_cash_usage_to_maturity_bps',
            'net_carry_to_maturity_bps',
            'net_carry_to_maturity % Rtn',
            'net_carry_to_maturity_default_bps',
            'net_carry_to_maturity_default % Rtn',
            'basis_hedges'
        ]
        simple_df = self.df[simplified_cols]
        simple_df = simple_df.rename(columns={'12mR+C-Same Series': 'RC_bps'})
        simple_df.columns = simple_df.columns.str.replace('_', ' ', regex=False)

        # sort by columns
        simple_df.sort_values(by='RC SIGNAL', ascending=False, inplace=True)


        return simple_df


