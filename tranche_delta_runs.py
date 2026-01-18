import pandas as pd
import sys
import os

directory = os.getcwd()
tranche_delta_file_path = os.path.join(directory, 'inputs', 'inputs.xlsx')

def tranche_deltas():
    df = pd.read_excel(tranche_delta_file_path, sheet_name='tranche_deltas')
    return df

def backup_tranche_deltas(df, dictionary_finder, attachment_detachment, current_series):
    filtered_df_sub_set = df[(df['index_short_name_generic'] == dictionary_finder) & (df['attachment-detachment'] == attachment_detachment)]
    filtered_df_sub_set['series_diff'] = filtered_df_sub_set['index_series'] - current_series
    tranche_delta = df.loc[filtered_df_sub_set['series_diff'].idxmin(),'delta']

    return tranche_delta
