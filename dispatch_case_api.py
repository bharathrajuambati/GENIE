
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 15:17:26 2023
 
@author: BAmbati
"""
# Import necessary libraries
import pandas as pd
import requests
import pytz
from pandas import json_normalize
import datetime

api_data_cache = None
# Configuration
cert = 'bambati-imowa.crt'
key = 'BAMBATI-IMOWA.key'
timezone = 'Australia/Perth'
def get_rounded_current_time():
   current_time = datetime.datetime.now(pytz.timezone(timezone))
   return current_time.replace(minute=(current_time.minute // 5) * 5, second=0, microsecond=0)
def fetch_dispatch_case_data():
   # Fetch Dispatch Case Data
   d_scenario = 'Reference'
   primary_di = get_rounded_current_time()
   end_time = primary_di  # Different end time for this API call
   response = requests.get(
       url='https://apis.prod.aemo.com.au:9319/WEM/v1/dispatchCase/dispatchData',
       proxies={'https': 'http://nemproxy:8080'},
       headers={'x-initiatingParticipantId': 'IMOWA', 'x-market': 'WEM'},
       cert=(cert, key),
       params={
           'DispatchScenario': d_scenario,
           'primaryDispatchInterval': primary_di,
           'dispatchIntervalStartDate': primary_di,
           'dispatchIntervalEndDate': end_time
       }
   )
   if response.status_code != 200:
       raise Exception(f"API call failed with status code: {response.status_code}")
   df_dispatch_case = pd.json_normalize(response.json(), record_path=['data', 'caseData'], meta=[['data', 'primaryDispatchInterval']])
   df_dispatch_case['dispatchInterval'] = pd.to_datetime(df_dispatch_case['dispatchInterval']).dt.strftime('%H:%M')
   # Pull Inflexible-flag data for PDI along with maxInjectionCapacity and maxWithdrawalCapacity
   facility_data_list = []
   # Loop through each row in df_dispatch_case
   for index, row in df_dispatch_case.iterrows():
       dispatch_interval = row['dispatchInterval']
       facilities_data = row['markets.energy.facilities']
       # Check if facilities_data is not empty and not NaN
       if facilities_data and not pd.isna(facilities_data).all():
           # Loop through each facility in the facilities_data
           for facility in facilities_data:
               facility_data_list.append({
                   'facilityCode': facility.get('facilityCode'),
                   'inflexibleFlag': facility.get('inflexibleFlag'),
                   'maxInjectionCapacity': facility.get('maxInjectionCapacity'),
                   'maxWithdrawalCapacity': facility.get('maxWithdrawalCapacity')
               })
   # Create DataFrame with desired columns
   df_inflexibleflag = pd.DataFrame(facility_data_list)
   # Extract facilityClass data for the first interval
   first_row_registration_data = df_dispatch_case.iloc[0]['registrationData']
   facility_class_data = []
   for facility in first_row_registration_data:
       facility_class_data.append({
           'facilityCode': facility['facilityCode'],
           'facilityClass': facility['facilityStandingData']['facilityClass']
       })
   df_facility_class = pd.DataFrame(facility_class_data)
   # Replace specific values in facilityClass column
   df_facility_class['facilityClass'] = df_facility_class['facilityClass'].replace({
       'SF': 'Scheduled',
       'SSF': 'Semi-Scheduled',
       'NSF': 'Non-Scheduled',
       'IL': 'Interruptible Load',
       'DSP': 'Demand Side Program'
   })
   # Pull Facility Bidding data for PDI
   # Initialize an empty list to store the data
   data = []
   # Loop through each row in df_dispatch_case
   for index, row in df_dispatch_case.iterrows():
       dispatch_interval = row['dispatchInterval']
       # Check if 'markets' key exists in the row
       if 'markets' in row:
           energy_data = row['markets'].get('energy', None)
           # Check if energy_data is not None and not empty
           if energy_data and not pd.isna(energy_data).all():
               facilities = energy_data.get('facilities', [])
               for facility_data in facilities:
                   facility = {
                       'facilityCode': facility_data.get('facilityCode', None),
                       'tranches': facility_data.get('tranches', None)
                   }
                   # Only add data if facility_code and tranches are present
                   if facility['facilityCode'] and facility['tranches']:
                       data.append(facility)
   # Assuming you have the 'facilities_data' list in your variable explorer
   # Initialize an empty list to store the data
   data = []
   # Iterate through facilities_data and extract desired information
   for facility_data in facilities_data:
       facility = {
           'facilityCode': facility_data.get('facilityCode', None),
           'IN-SERVICE': 0,
           'AVAILABLE': 0
       }
       # Handle 'tranches' data within the facility
       tranches = facility_data.get('tranches', [])
       for tranche_data in tranches:
           capacity_type = tranche_data.get('capacityType', None)
           quantity = tranche_data.get('quantity', None)
           # Check capacity type and update quantities accordingly
           if capacity_type == 'IN-SERVICE':
               facility['IN-SERVICE'] += quantity
           elif capacity_type == 'AVAILABLE':
               facility['AVAILABLE'] += quantity
       # Append facility data as a dictionary to the list
       data.append(facility)
   # Create the DataFrame from the list of dictionaries
   df_facility_bidding = pd.DataFrame(data)
   return df_dispatch_case, df_inflexibleflag, df_facility_bidding, df_facility_class

def update_api_data_cache():
   global api_data_cache
   api_data_cache = fetch_dispatch_case_data()
df_dispatch_case, df_inflexibleflag, df_facilitybidding,df_facility_class = fetch_dispatch_case_data()