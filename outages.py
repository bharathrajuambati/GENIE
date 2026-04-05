# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 14:54:53 2024

@author: BAmbati
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Nov 25 14:17:14 2023
@author: BAmbati
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
# SSL client certificates
cert = 'bambati-imowa.crt'
key = 'BAMBATI-IMOWA.key'
def round_time_to_nearest_interval(dt, interval_minutes=5):
   """
   Rounds a datetime object to the nearest interval.
   """
   new_minute = (dt.minute // interval_minutes) * interval_minutes
   return dt.replace(minute=new_minute, second=0, microsecond=0)
def get_rounded_midnight_time(timezone_name):
   timezone = pytz.timezone(timezone_name)
   today_midnight = datetime.combine(datetime.today(), datetime.min.time())
   today_midnight_tz = timezone.localize(today_midnight)
   return round_time_to_nearest_interval(today_midnight_tz)
# Timezone for Australia/Perth
australia_timezone = 'Australia/Perth'
# Get today's date at midnight in the Australia timezone and round it to the nearest 5-minute interval
outage_period_start = get_rounded_midnight_time(australia_timezone)
outage_period_end = outage_period_start + timedelta(days=2)
# Format the times as ISO 8601 strings
formatted_start_time = outage_period_start.isoformat()
formatted_end_time = outage_period_end.isoformat()

def truncate_string(s, length=50):
   """
   Truncate a string to a specified length, adding ellipsis if necessary.
   """
   if len(s) > length:
       return s[:length] + '...'
   return s
def fetch_and_process_network_outages():
   """
   Fetch and process data for Network outages.
   """
   response = requests.get(
       url='https://apis.prod.aemo.com.au:9319/WEM/v1/outageManagement/outageData',
       proxies={'https': 'http://nemproxy:8080'},
       headers={'x-initiatingParticipantId': 'IMOWA', 'x-market': 'WEM'},
       cert=(cert, key),
       params={
           'outageSource': 'Network',
           'periodStart': formatted_start_time,
           'periodEnd': formatted_end_time,
           'pageNumber': 1,
           'pageSize': 1000
       }
   )
   json_data = response.json()
   outages_data = json_data['data']['outages']
   df_network_outages = pd.json_normalize(outages_data)
   # Convert to datetime and format the columns
   datetime_cols = ['duration.commencementInterval', 'duration.endInterval']
   for col in datetime_cols:
       if df_network_outages[col].dtype == 'object':
           df_network_outages[col] = pd.to_datetime(df_network_outages[col])
   df_network_outages['duration.commencementInterval'] = df_network_outages['duration.commencementInterval'].dt.strftime('%d/%m/%Y %H:%M')
   df_network_outages['duration.endInterval'] = df_network_outages['duration.endInterval'].dt.strftime('%d/%m/%Y %H:%M')
   # Handle NaT values if needed (optional, based on your data)
   df_network_outages['duration.commencementInterval'] = df_network_outages['duration.commencementInterval'].replace('NaT', '')
   df_network_outages['duration.endInterval'] = df_network_outages['duration.endInterval'].replace('NaT', '')
   # Function to extract equipment IDs from each row
   def extract_equipment_ids(equipment_list):
       return ', '.join([equipment['equipmentId'] for equipment in equipment_list])
   # Apply this function to the equipment list column
   df_network_outages['equipments.equipmentList'] = df_network_outages['equipments.equipmentList'].apply(extract_equipment_ids)
   # Truncate the equipment list strings
   df_network_outages['equipments.equipmentList'] = df_network_outages['equipments.equipmentList'].apply(lambda x: truncate_string(x, length=80))
   # Function to extract point of isolation values from each row
   def extract_isolation_points(isolation_points_list):
       return ', '.join([point['pointOfIsolation'] for point in isolation_points_list])
   # Apply this function to the pointOfIsolation.points column
   df_network_outages['pointOfIsolation.points'] = df_network_outages['pointOfIsolation.points'].apply(extract_isolation_points)
   # Truncate the equipment list strings
   df_network_outages['pointOfIsolation.points'] = df_network_outages['pointOfIsolation.points'].apply(lambda x: truncate_string(x, length=50))
   # Define columns to display for the network outages DataFrame
   network_columns = [
       'details.outageNumber',
       'duration.commencementInterval',
       'duration.endInterval',
       'equipments.equipmentList',
       'pointOfIsolation.points',
       'details.outageType',
       'equipments.secondaryEquipmentFlag',
       'permits.permitType',
       'temporaryRestoration.temporaryRestoredFlag',
       'contingencyPlanInformation.estimatedRecoveryHours',
       'contingencyPlanInformation.estimatedRecoveryMinutes'
   ]
   # Sort and filter columns
   df_network_outages_sorted = df_network_outages[network_columns].copy()
   # Rename columns
   network_columns_rename = {
       'details.outageNumber': 'MPI',
       'duration.commencementInterval': 'Start',
       'duration.endInterval': 'End',
       'equipments.equipmentList': 'Equipment',
       'pointOfIsolation.points': 'Isolation Points',
       'details.outageType': 'Outage Type',
       'equipments.secondaryEquipmentFlag': 'Secondary Equip',
       'permits.permitType': 'Permit Type',
       'temporaryRestoration.temporaryRestoredFlag': 'Overnight',
       'contingencyPlanInformation.estimatedRecoveryHours': 'Recall Hrs',
       'contingencyPlanInformation.estimatedRecoveryMinutes': 'Recall Minutes',
   }
   df_network_outages_sorted.rename(columns=network_columns_rename, inplace=True)
   return df_network_outages_sorted
def fetch_and_process_generator_outages():
   """
   Fetch and process data for Generator outages.
   """
   response = requests.get(
       url='https://apis.prod.aemo.com.au:9319/WEM/v1/outageManagement/outageData',
       proxies={'https': 'http://nemproxy:8080'},
       headers={'x-initiatingParticipantId': 'IMOWA', 'x-market': 'WEM'},
       cert=(cert, key),
       params={
           'outageSource': 'Generator',
           'periodStart': formatted_start_time,
           'periodEnd': formatted_end_time,
           'pageNumber': 1,
           'pageSize': 1000
       }
   )
   json_data = response.json()
   outages_data = json_data['data']['outages']
   df_generator_outages = pd.json_normalize(outages_data)
   # Convert to datetime and format columns
   datetime_cols = ['duration.commencementInterval', 'duration.endInterval']
   for col in datetime_cols:
       if df_generator_outages[col].dtype == 'object':
           df_generator_outages[col] = pd.to_datetime(df_generator_outages[col])
   df_generator_outages['duration.commencementInterval'] = df_generator_outages['duration.commencementInterval'].dt.strftime('%d/%m/%Y %H:%M')
   df_generator_outages['duration.endInterval'] = df_generator_outages['duration.endInterval'].dt.strftime('%d/%m/%Y %H:%M')
   # Handle NaT values if needed (optional, based on your data)
   df_generator_outages['duration.commencementInterval'] = df_generator_outages['duration.commencementInterval'].replace('NaT', '')
   df_generator_outages['duration.endInterval'] = df_generator_outages['duration.endInterval'].replace('NaT', '')
   # Preprocess DataFrame
   df_generator_outages = df_generator_outages.fillna('Not Available')  # Replace NaN values
   # Convert columns with lists to string
   for col in df_generator_outages.columns:
       if isinstance(df_generator_outages[col][0], (list, dict)) or df_generator_outages[col].isna().any():
           df_generator_outages[col] = df_generator_outages[col].apply(lambda x: str(x))
   # Function to flatten essential system services
   def flatten_essential_system_services(row):
       # Check if the row is a list and not empty
       if isinstance(row['serviceReduction.essentialSystemServices'], list) and row['serviceReduction.essentialSystemServices']:
           # Create a string to hold the aggregated data
           aggregated_data = []
           # Iterate through each item in the list
           for item in row['serviceReduction.essentialSystemServices']:
               serviceType = item.get('serviceType', 'Not Available')
               serviceAvailability = item.get('serviceAvailability', 'Not Available')
               serviceRAC = item.get('serviceRAC', 0)
               # Format and add the data to the aggregated string
               aggregated_data.append(f"{serviceType} ({serviceAvailability}, RAC: {serviceRAC})")
           # Join all the entries with a separator
           return ', '.join(aggregated_data)
       else:
           return 'Not Available'
   # Apply the function to the DataFrame
   df_generator_outages['EssentialSystemServices'] = df_generator_outages.apply(flatten_essential_system_services, axis=1)
   # Define columns to display for the generator outages Data Frame
   generator_columns = [
       'details.outageNumber',
       'duration.commencementInterval',
       'duration.endInterval',
       'serviceReduction.facilityCode',
       'serviceReduction.facilityRAC',
       'details.outageType',
       'details.outageStatus',
       'details.lastModifiedUser',
       'details.originator',
       'contingencyPlanInformation.estimatedRecoveryHours',
       'contingencyPlanInformation.estimatedRecoveryMinutes',
   ]
   # Sort and filter columns
   df_generator_outages_sorted = df_generator_outages[generator_columns].copy()
   # Rename columns
   generator_columns_rename = {
       'details.outageNumber': 'MPI',
       'duration.commencementInterval': 'Start',
       'duration.endInterval': 'End', 
       'serviceReduction.facilityCode': 'facilityCode',
       'serviceReduction.facilityRAC': 'RAC',
       'details.outageType': 'Outage Type',
       'details.outageStatus': 'Status',
       'details.lastModifiedUser': 'Last Modified',
       'details.originator': 'Originator',
       'contingencyPlanInformation.estimatedRecoveryHours': 'Recall Hrs',
       'contingencyPlanInformation.estimatedRecoveryMinutes': 'Recall Minutes',
   }
   df_generator_outages_sorted.rename(columns=generator_columns_rename, inplace=True)
   return df_generator_outages_sorted
df_network_outages_sorted = fetch_and_process_network_outages()
df_generator_outages_sorted = fetch_and_process_generator_outages()