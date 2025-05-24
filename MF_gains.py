# %%
import re
import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime, timedelta

# %%
df = pd.read_csv('cams_mf.csv')

# %%
df.sample(5)

# %%
df['Fund Name'] = df['Name'] + ' - ' + df['Investment Channel'] + ' Plan - ' + df['Fund Type']

# %%
df.columns

# %%
all_mf = pd.DataFrame(json.loads(requests.get('https://api.mfapi.in/mf').content.decode()))
found_mfs = all_mf[all_mf['isinGrowth'].isin(df['ISIN'].unique())]

# %%
history_df = pd.DataFrame()

for i, j in found_mfs.iterrows():
    print(j['schemeCode'], j['schemeName'], j['isinGrowth'])
    temp = json.loads(requests.get(f'https://api.mfapi.in/mf/{j['schemeCode']}').content.decode())
    temp_df = pd.DataFrame(temp['data'])
    temp_df['scheme_name'] = temp['meta']['scheme_name']
    temp_df['isin'] = temp['meta']['isin_growth']
    history_df = pd.concat([history_df, temp_df])

history_df['date'] = history_df['date'].apply(lambda x: '-'.join(x.split('-')[::-1]))
history_df['nav'] = pd.to_numeric(history_df['nav'])

# %%
history_df.sort_values(by=['isin', 'date'], ascending=False, inplace=True)
today_nav_df = history_df.groupby('isin').first().reset_index()
today_nav_df.columns = ['isin', 'date_last', 'nav_last', 'scheme_name']
today_nav_df.drop(columns='scheme_name', inplace=True)
history_df = history_df[['date', 'nav', 'scheme_name', 'isin']]
# history_df = history_df.merge(today_nav_df, how='left', on='isin', suffixes=('', '_last'))

# %%
df = df.merge(history_df, left_on=['Date', 'ISIN'], right_on=['date', 'isin'], suffixes=('', '_history'), how='left')


# %%
df = df.merge(today_nav_df, left_on=['ISIN'], right_on=['isin'], suffixes=('', 'last'), how='left')

# %%
issues = df[df['Price']!=df['nav']]
issues.loc[:, 'diff'] = issues['Price'] - issues['nav']
issues = issues[['Name', 'Date', 'Amount', 'Units', 'Price', 'date',
       'nav', 'diff', 'Unit_balance', 'Investment Type', 'Fund Type', 'Investment Channel',
       'Folio No', 'ISIN', 'Advisor', 'Advisor Name', 'AMC', 'Remarks', 'Fund Name', 'scheme_name', 'isin', 'nav_last', 'date_last']]

# %%
df['today_value'] = df['Units'] * df['nav_last']
df['difference'] = df['today_value'] - (df['Units'] * df['Price'])
df['duration'] = datetime.now() - pd.to_datetime(df['Date'])
df['duration'] = df['duration'].dt.days
df['gain_type'] = np.where(df['duration'] > 366, 'Long Term', 'Short Term')

# %%
df.drop(columns=['date', 'scheme_name', 'isin', 'isinlast'], inplace=True, errors='ignore')

# %%
df.to_csv('mutual_fund.csv', index=False)
df = pd.read_csv('mutual_fund.csv')

# %%
red_df = df[df['Investment Type'] == 'Redemption']
pur_df = df[df['Investment Type'] != 'Redemption']

# %%
pur_df.loc[:, 'units_left'] = pur_df['Units']
pur_df.loc[:, 'sell_date'] = ''
red_df.sort_values('Date', inplace=True)
pur_df.sort_values('Date', inplace=True)
pur_df.reset_index(drop=True, inplace=True)
for i, j in red_df.iterrows():
    units_sold = j['Units']
    sell_date = j['Date']
    to_op = pur_df[(pur_df['ISIN'] == j['ISIN']) & (pur_df['Date'] <= j['Date']) & (pur_df['units_left'] > 0)][['Fund Name', "Date", "units_left"]].index.to_list()
    for idx in to_op:
        units_left = pur_df.loc[idx, 'units_left']
        pur_df.loc[idx, 'units_left'] = max(0, units_sold + units_left)
        pur_df.loc[idx, 'sell_date'] = sell_date
        units_sold = min(0, units_sold + units_left)
        if units_sold == 0:
            break
    if round(units_sold, 5) != 0:
        print(j)
        print(units_sold)
        print(pur_df.loc[to_op])




# %%
pur_df['today_value'] = pur_df['units_left'] * pur_df['nav_last']
pur_df['difference'] = pur_df['today_value'] - (pur_df['units_left'] * pur_df['Price'])
pur_df['duration'] = datetime.now() - pd.to_datetime(pur_df['Date'])
pur_df['duration'] = pur_df['duration'].dt.days
pur_df['gain_type'] = np.where(pur_df['duration'] > 366, 'Long Term', 'Short Term')

# %%
pur_df.to_csv('mf_gains.csv', index=False)


