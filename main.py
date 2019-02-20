# Note: Official DeribitApi github repo is outdated, local file to incorporate latest rest API changes
from DeribitApi import RestClient
import DeribitUtil
import os
import pandas as pd
import time
from datetime import datetime
import numpy as np

"""
Note, UTC!!!!
"""

################ CHANGE THIS ##################
api_key = "5U9bFSyFgaMzV"
api_secret = "ESG7L6D7YD4VWDDL2BBQETVLNSJA3HLR"
###############################################

client = RestClient(api_key, api_secret)
today = datetime.today() # GMT+1 (note, Deribit uses GMT+0)

# Get all current instruments
all_instruments_lst = client.getinstruments()
# We only want options
all_current_option_instruments = [x['instrumentName'] for x in all_instruments_lst if x['kind'] == 'option']
print(all_current_option_instruments)

for contract in all_current_option_instruments:

    outdir = "Data/{}".format(contract)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
        prev_tickdata = pd.DataFrame()
        last_timestamp = None
    else:
        try:
            prev_tickdata = pd.read_csv(outdir + "/tickdata.csv")
            last_timestamp = prev_tickdata.iloc[-1]['timeStamp']
        except Exception:
            # rare case where a directory is be empty
            prev_tickdata = pd.DataFrame()
            last_timestamp = None

    # Deribit returns a maximum of 1000 ticks
    last_trades = client.getlasttrades(contract, count=1000, start_timestamp=last_timestamp)
    while len(last_trades) == 1000:
        # there are more than 1000ticks for this contract;
        # -> get remaining ticks
        # last trades is a sorted list where the last element is the oldest received tick
        end_timestamp = last_trades[-1]['timeStamp']
        # get ticks up until last end_timestamp
        last_trades += client.getlasttrades(contract, count=1000, end_timestamp=end_timestamp)

    # Sometimes a contract doesn't have trades; on to the next contract
    if len(last_trades) == 0:
        continue
    new_data = pd.DataFrame(last_trades)
    new_data['date_utc'] = pd.to_datetime(new_data['timeStamp'].apply(lambda x: DeribitUtil.from_timestamp(x)))

    df = pd.DataFrame([item for item in new_data['instrument'].apply(lambda x: x.split("-"))],
                      columns=['ticker', 'expirationDate', 'strike', 'right'])
    new_data = pd.concat([df, new_data], axis=1)

    new_data = pd.concat([prev_tickdata, new_data], axis=0)
    new_data = new_data.drop_duplicates(subset='tradeId', keep='last')
    new_data = new_data.sort_values('date_utc')
    new_data = new_data.set_index('date_utc')

    # In principle a lot of redundant work for existing ohcl data, but data is so sparse is okay for now
    ohcl = new_data['price'].groupby(pd.Grouper(freq='D')).transform(np.cumsum).resample('D', how='ohlc')
    iv_close = new_data['iv'].groupby(pd.Grouper(freq='D')).resample('D').apply(lambda x:x.iloc[-1])
    vol = new_data['price'].groupby(pd.Grouper(freq='D')).count()
    ohcl['iv'] = iv_close
    ohcl['volume'] = vol

    new_data.to_csv(outdir+"/tickdata.csv", index=True)
    ohcl.to_csv(outdir+"/ohcl_daily.csv")

    # max 200 req per second
    time.sleep(0.008)

