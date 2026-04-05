## -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 15:03:17 2024

@author: BAmbati
"""
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 10:58:14 2024
@author: BAmbati
"""
# Import necessary libraries
import pandas as pd
import requests
import datetime
import pytz
from pandas import json_normalize
# Constants and setup
CERT_PATH = 'bambati-imowa.crt'
KEY_PATH = 'BAMBATI-IMOWA.key'
TIMEZONE = 'Australia/Perth'
DISPATCH_URL = 'https://apis.prod.aemo.com.au:9319/WEM/v1/dispatchSolution/dispatchData'
PROXIES = {'https': 'http://nemproxy:8080'}
HEADERS = {'x-initiatingParticipantId': 'IMOWA', 'x-market': 'WEM'}
def get_rounded_current_time():
   """Get the current time rounded to the nearest 5 minutes."""
   current_time = datetime.datetime.now(pytz.timezone(TIMEZONE))
   return current_time.replace(minute=(current_time.minute // 5) * 5, second=0, microsecond=0)
def call_dispatch_solution():
   """Call the dispatch solution API and return the normalized data."""
   d_scenario = 'Reference'
   primary_di = get_rounded_current_time()
   end_time = primary_di + datetime.timedelta(hours=2)  # Adjust end time as needed for this API call
   response = requests.get(
       url=DISPATCH_URL,
       proxies=PROXIES,
       headers=HEADERS,
       cert=(CERT_PATH, KEY_PATH),
       params={
           'DispatchScenario': d_scenario,
           'primaryDispatchInterval': primary_di,
           'dispatchIntervalStartDate': primary_di,
           'dispatchIntervalEndDate': end_time
       }
   )
   if response.status_code == 200:
       return pd.json_normalize(response.json(), record_path=['data', 'solutionData'], meta=[['data', 'primaryDispatchInterval']])
   else:
       raise Exception(f"API call failed with status code: {response.status_code}")
def process_dispatch_data(df_solution):
   """Process the dispatch solution data into various DataFrames."""
   DIs = pd.to_datetime(df_solution['dispatchInterval'], format='%H:%M')
   DIs = DIs.dt.strftime('%H:%M') 
   # Initialize DataFrames to store market data
   df_CR_full = pd.DataFrame()
   df_CL_full = pd.DataFrame()
   df_RR_full = pd.DataFrame()
   df_RL_full = pd.DataFrame()
   df_energy_full = pd.DataFrame()
   df_rocof_full = pd.DataFrame()
   df_faststartflag = pd.DataFrame()
   for n, json in enumerate(df_solution['schedule']):
       df_temp = pd.json_normalize(json).set_index('marketService')
       # Process Contingency Raise
       df_CR_full = join_market_data(df_temp, 'contingencyRaise', df_CR_full, DIs[n])
       # Process Contingency Lower
       df_CL_full = join_market_data(df_temp, 'contingencyLower', df_CL_full, DIs[n])
       # Process Regulation Raise
       df_RR_full = join_market_data(df_temp, 'regulationRaise', df_RR_full, DIs[n])
       # Process Regulation Lower
       df_RL_full = join_market_data(df_temp, 'regulationLower', df_RL_full, DIs[n])
       # Process Energy
       df_energy_full = join_market_data(df_temp, 'energy', df_energy_full, DIs[n])
       # Process ROCOF
       df_rocof_full = join_market_data(df_temp, 'rocof', df_rocof_full, DIs[n])
   df_faststartflag = process_faststart_flag(df_solution, DIs)
   df_congestion_rental_full = process_congestion_rental(df_solution, DIs)
   # Define a function to format numbers by rounding to the nearest integer
   def format_numbers_without_decimals(x):
      if isinstance(x, (int, float)):
          return round(x)
      else:
          return x  # Return non-numeric values as they are   
   # Apply the formatting function to your DataFrames
   df_CR_full = df_CR_full.map(format_numbers_without_decimals)
   df_CL_full = df_CL_full.map(format_numbers_without_decimals)
   df_RR_full = df_RR_full.map(format_numbers_without_decimals)
   df_RL_full = df_RL_full.map(format_numbers_without_decimals)
   df_energy_full = df_energy_full.map(format_numbers_without_decimals)
   df_rocof_full = df_rocof_full.map(format_numbers_without_decimals)
   
   
   
   
   
   
   
   
   
   
   
   
   return df_CR_full, df_CL_full, df_RR_full, df_RL_full, df_energy_full, df_rocof_full, df_faststartflag, df_congestion_rental_full
def join_market_data(df_temp, market_service, df_full, DI):
   """Join market data into the full DataFrame."""
   if market_service in df_temp.index:
       df_market = pd.DataFrame(df_temp.loc[market_service, :].iloc[0]).set_index('facilityCode')[['quantity']]
       df_market.columns = [DI]
       if df_full.empty:
           df_full = df_market
       else:
           df_full = df_full.join(df_market, how='outer')
   return df_full
def process_faststart_flag(df_solution, DIs):
   """Process the fast start flag data."""
   df_faststartflag = pd.DataFrame()
   for n, json in enumerate(df_solution['facilityScheduleDetails']):
       df_temp = pd.json_normalize(json)[['facilityCode', 'fastStartFlag']]
       df_temp['dispatchInterval'] = DIs[n]
       if df_faststartflag.empty:
           df_faststartflag = df_temp
       else:
           df_faststartflag = pd.concat([df_faststartflag, df_temp], ignore_index=True)
   return df_faststartflag.pivot(index='facilityCode', columns='dispatchInterval', values='fastStartFlag')
def process_congestion_rental(df_solution, DIs):
   """Process the congestion rental data."""
   df_congestion_rental_full = pd.DataFrame()
   for n, json in enumerate(df_solution['facilityScheduleDetails']):
       df_temp = pd.json_normalize(json).set_index('facilityCode')
       if 'congestionRental' in df_temp.columns:
           df_congestion_rental = pd.DataFrame(df_temp['congestionRental'])
           df_congestion_rental.columns = [DIs[n]]
           if df_congestion_rental_full.empty:
               df_congestion_rental_full = df_congestion_rental
           else:
               df_congestion_rental_full = df_congestion_rental_full.join(df_congestion_rental, how='outer')
   return df_congestion_rental_full
def replace_values(cell):
   """Replace values in the congestion rental DataFrame."""
   if cell > 0:
       return 'Positive'
   elif cell < 0:
       return 'Negative'
   else:
       return '0'


def main():
   """Main function to execute the data processing workflow and return DataFrames."""
   df_solution = call_dispatch_solution()
   df_solution['dispatchInterval'] = pd.to_datetime(df_solution['dispatchInterval']).dt.strftime('%H:%M')
   df_CR_full, df_CL_full, df_RR_full, df_RL_full, df_energy_full, df_rocof_full, df_faststartflag, df_congestion_rental_full = process_dispatch_data(df_solution)
   df_congestion_rental_processed = df_congestion_rental_full.map(replace_values)
   return df_CR_full, df_CL_full, df_RR_full, df_RL_full, df_energy_full, df_rocof_full, df_faststartflag, df_congestion_rental_processed
if __name__ == "__main__":
   # Call the main function and assign the returned DataFrames to global variables
   df_CR_full, df_CL_full, df_RR_full, df_RL_full, df_energy_full, df_rocof_full, df_faststartflag, df_congestion_rental_processed = main()
