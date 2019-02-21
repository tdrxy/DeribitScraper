# Note: Official DeribitApi github repo is outdated, local file to incorporate latest rest API changes
import Credentials

import JoinAllFiles
from DeribitApi import RestClient
import os
import pandas as pd
import time
from datetime import datetime


class DeribitOptionScraper():

    def __init__(self, api_key, api_secret, outdir="Data/"):
        self.client = RestClient(api_key, api_secret)
        self.sleep_interval = 0.008  # max 200 requests => 0.005; so some extra margin
        self.outdir = outdir

    def update_tick_data(self):
        """
        Requests all live Deribit option contracts and writes latest tick data per contract to disk.
        It also aggregates this tick data into daily option data
        :return: None
        """

        # Get all current instruments
        all_instruments_lst = self.client.getinstruments()
        # We only want options
        all_current_option_instruments = [x['instrumentName'] for x in all_instruments_lst if x['kind'] == 'option']

        print("Fetching tick data of {} contracts.".format(len(all_current_option_instruments)))

        all_current_option_instruments = \
            sorted(all_current_option_instruments,
                   key=lambda x: datetime.strptime(
                       self._sort_helper(x.split("-")[1]), '%d%B%Y'))

        for contract in all_current_option_instruments:
            print("Doing {}".format(contract))
            outdir = self.outdir+contract

            prev_tickdata, last_timestamp = self._check_previous_data(outdir)

            # Deribit returns a maximum of 1000 ticks
            last_trades = self.client.getlasttrades(contract, count=1000, start_timestamp=last_timestamp)
            while len(last_trades) == 1000:
                # there are more than 1000ticks for this contract;
                # -> get remaining ticks
                # last trades is a sorted list where the last element is the oldest received tick
                end_timestamp = last_trades[-1]['timeStamp']
                # get ticks up until last end_timestamp
                last_trades += self.client.getlasttrades(contract, count=1000, end_timestamp=end_timestamp)

            # Sometimes a contract doesn't have trades; on to the next contract
            if len(last_trades) == 0:
                continue

            new_data = self._update_and_write_tick_data(last_trades, prev_tickdata, outdir)
            self._write_daily_aggregation(new_data, outdir)

            # max 200 req per second
            time.sleep(self.sleep_interval)

    def _sort_helper(self, x):
        """
        convert 20FEB19 => 20Februari2019
        :param x:
        :return:
        """
        conf = {'JAN': 'January', 'FEB': 'February', 'MAR': "March",
                'APR': "April", "MAY": "May", "JUN": "June", "JUL": "July",
                "AUG": "August", "SEP": "September", "OCT": "October",
                "NOV": "November", "DEC": "December"}

        days = x[0:-5]
        month = x[-5:-2]
        month = conf[month]
        year = "20" + x[-2:]

        return days + month + year

    def _check_previous_data(self, outdir):
        """
        Check whether there is previously scraped data in given directory.
        If so, return last timestamp so we don't request duplicate data.
        :param outdir:
        :return:
        """
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
        return prev_tickdata, last_timestamp

    def _update_and_write_tick_data(self, last_trades, prev_tickdata, outdir):
        """
        Merges previously scraped data with new data scraped today. Results in a file Data/<contract>/tickdata.csv
        :param last_trades:
        :param prev_tickdata:
        :param outdir:
        :return:
        """
        new_data = pd.DataFrame(last_trades)
        new_data = pd.concat([prev_tickdata, new_data], axis=0, sort=False)
        new_data['date_utc'] = pd.to_datetime(
            new_data['timeStamp'].apply(lambda x: datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%dT%H:%M:%S.%f'))
        )
        new_data = new_data.drop_duplicates(subset='tradeId', keep='last')
        new_data = new_data.sort_values('date_utc')
        new_data = new_data.set_index('date_utc')
        new_data.to_csv(outdir + "/tickdata.csv", index=True)
        return new_data

    def _write_daily_aggregation(self, new_data, outdir):
        """
        Aggregates option tick data per contract. Resulting in a Data/<contract>/ohcl_daily.csv file
        :param new_data:
        :param outdir:
        :return:
        """
        ohcl = new_data['price'].groupby(pd.Grouper(freq='D')).resample('D').apply(lambda x: x.iloc[-1])
        ohcl.index = ohcl.index.get_level_values(0)
        iv_close = new_data['iv'].groupby(pd.Grouper(freq='D')).resample('D').apply(lambda x: x.iloc[-1])
        vol = new_data['price'].groupby(pd.Grouper(freq='D')).resample('D').count()
        close_underlying = new_data['indexPrice'].groupby(pd.Grouper(freq='D')).resample('D').apply(
            lambda x: x.iloc[-1])
        instruments = pd.Series(v for v in ([new_data.reset_index()['instrument'][0]] * len(ohcl)))

        ohcl_old_index = ohcl.index
        ohcl = pd.concat([pd.DataFrame(ohcl.values, columns=['close_btc']),
                          pd.DataFrame(iv_close.values, columns=['iv']),
                          pd.DataFrame(vol.values, columns=['volume']),
                          pd.DataFrame(close_underlying.values, columns=['underlyingPrice']),
                          pd.DataFrame(instruments.values, columns=['contract'])],
                         axis=1)
        ohcl.index = ohcl_old_index

        df = pd.DataFrame([item for item in instruments.apply(lambda x: x.split("-"))],
                          columns=['ticker', 'expirationDate', 'strike', 'right'])
        df.index = ohcl.index
        ohcl = pd.concat([df, ohcl], axis=1)
        ohcl = ohcl.fillna(0)
        ohcl.to_csv(outdir + "/daily_agg.csv", index=True)


if __name__ == "__main__":

    ################ CHANGE THIS ##################
    api_key = Credentials.apikey
    api_secret = Credentials.apisecret
    ###############################################

    scraper = DeribitOptionScraper(api_key, api_secret, outdir="Data/")
    scraper.update_tick_data()
    JoinAllFiles.run()
    print("Done")
