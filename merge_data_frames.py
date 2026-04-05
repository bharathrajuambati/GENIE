# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 08:03:45 2024

@author: BAmbati
"""


import pandas as pd

from fetch_facility_data import fetch_facility_data

from dispatch_case_api import fetch_dispatch_case_data

from connect_to_aemo_pi import connect_to_aemo_pi

from tag_dict import tag_dict

from tolerance_values import tolerance_values

def merge_data_frames():

    # Connect to AEMO PI

    pi = connect_to_aemo_pi()

    # Fetch data from the dispatch case API

    df_dispatch_case, df_inflexibleflag, df_facility_bidding, df_facility_class = fetch_dispatch_case_data()

    # Fetch facility data from the PI system

    df_facility_data = fetch_facility_data(pi, tag_dict, tolerance_values)

    # Merge the data frames

    # Assuming 'facilityCode' is the common column to merge on

    df_dashboard = df_facility_data.merge(df_inflexibleflag, on='facilityCode', how='left', suffixes=('', '_dispatch'))

    df_dashboard = df_dashboard.merge(df_facility_bidding, on='facilityCode', how='left', suffixes=('', '_bidding'))

    df_dashboard = df_dashboard.merge(df_facility_class, on='facilityCode', how='left', suffixes=('', '_class'))

 


    # Define the desired column order
    column_order = [
        'facilityCode', 'maxInjectionCapacity','AVAILABLE', 'IN-SERVICE','Nett MW', 'Dispatch Target', 'Delta', 'Tolerance',
        'Ramp Rate', 'RLM1', 'RLM2', 'RLM3', 'Reg L', 'Reg R', 'Cont L',
        'Cont R','inflexibleFlag','maxWithdrawalCapacity', 'facilityClass','Description'
    ]
    df_dashboard = df_dashboard[column_order]
    # Round all numerical columns to the nearest integer, handling NaN values
    numerical_columns = df_dashboard.select_dtypes(include=['number']).columns
    df_dashboard[numerical_columns] = df_dashboard[numerical_columns].fillna(0).round(0).astype(int)
    return df_dashboard
if __name__ == "__main__":
    df_dashboard = merge_data_frames()

   