import pandas as pd
from pi_tags import alarm_category

def fetch_pi_alarm_data(pi):
    df = pi.get_current_value(alarm_category)
    df_alarms = pd.DataFrame(df.T.iloc[0], columns=['Current value'])
    df_alarms.index.name = 'Index'
    return df_alarms
