# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 12:34:15 2024

@author: BAmbati
"""

import datetime
import pandas as pd
def fetch_facility_data(pi, tag_dict, tolerance_values):
    # Combine all necessary tags into a single list for the facility data
    facility_tags = [tag for attr_dict in tag_dict.values() for tag in attr_dict.values() if tag not in ["facilitycode", "description"]]
    updated_values = pi.get_current_value(facility_tags)
    if updated_values is None or updated_values.empty:
        print("Failed to fetch updated values from PI system.")
        return pd.DataFrame()
    data_for_df = []
    for facility, tags in tag_dict.items():
        nettmw_value = round(updated_values.get(tags["nettmw"]).iloc[0], 0) if "nettmw" in tags and updated_values.get(tags["nettmw"]) is not None else None
        dispatchtarget_value = round(updated_values.get(tags["dispatchtarget"]).iloc[0], 0) if "dispatchtarget" in tags and updated_values.get(tags["dispatchtarget"]) is not None else None
        ramprate_value = round(updated_values.get(tags["ramprate"]).iloc[0], 0) if "ramprate" in tags and updated_values.get(tags["ramprate"]) is not None else None
        delta_value = dispatchtarget_value - nettmw_value if nettmw_value is not None and dispatchtarget_value is not None else None
        tolerance_value = tolerance_values.get(facility)
        description = tags.get("description", "")
        # Fetch additional tags safely
        rlm1_value = round(updated_values.get(tags["RLM1"]).iloc[0], 0) if "RLM1" in tags and updated_values.get(tags["RLM1"]) is not None else None
        rlm2_value = round(updated_values.get(tags["RLM2"]).iloc[0], 0) if "RLM2" in tags and updated_values.get(tags["RLM2"]) is not None else None
        rlm3_value = round(updated_values.get(tags["RLM3"]).iloc[0], 0) if "RLM3" in tags and updated_values.get(tags["RLM3"]) is not None else None
        reg_l_value = round(updated_values.get(tags["Reg L"]).iloc[0], 0) if "Reg L" in tags and updated_values.get(tags["Reg L"]) is not None else None
        reg_r_value = round(updated_values.get(tags["Reg R"]).iloc[0], 0) if "Reg R" in tags and updated_values.get(tags["Reg R"]) is not None else None
        cont_l_value = round(updated_values.get(tags["Cont L"]).iloc[0], 0) if "Cont L" in tags and updated_values.get(tags["Cont L"]) is not None else None
        cont_r_value = round(updated_values.get(tags["Cont R"]).iloc[0], 0) if "Cont R" in tags and updated_values.get(tags["Cont R"]) is not None else None
        row = {
            "facilityCode": tags.get("facilitycode", ""),
            "Nett MW": nettmw_value,
            "Dispatch Target": dispatchtarget_value,
            "Delta": delta_value,
            "Tolerance": tolerance_value,
            "Ramp Rate": ramprate_value,
            "RLM1": rlm1_value,
            "RLM2": rlm2_value,
            "RLM3": rlm3_value,
            "Reg L": reg_l_value,
            "Reg R": reg_r_value,
            "Cont L": cont_l_value,
            "Cont R": cont_r_value,
            "Description": description,
        }
        data_for_df.append(row)
    df_facility_data = pd.DataFrame(data_for_df)
    df_facility_data.set_index('facilityCode', inplace=True)
    df_facility_data = df_facility_data.round(0)  # Optional: still round values before converting
    # Fill NaN with 0 and replace inf with 0 for numeric columns
    numeric_cols = df_facility_data.select_dtypes(include=['float', 'int']).columns
    df_facility_data[numeric_cols] = df_facility_data[numeric_cols].fillna(0).replace([float('inf'), -float('inf')], 0).astype(int)
    
    # Print the current system time when the update finishes
    #print(f"Facility data updated successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return df_facility_data


