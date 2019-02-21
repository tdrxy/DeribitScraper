# DeribitScraper
Retrieves option tick data for all live option contracts on Deribit.
Deribit limits history requests to 6-7 days. Run ever 2-3 days to update latest data. 


# How to use
- Set api_key and api_secret variables in main.py @ ~line 155. (string variables)
- Optional: change outdir variable to write results somewhere else (default: Data/)
- Run: venv/bin/python main.py (In directory /DeribitScraper)

# Result
Each live option contract its raw (unprocessed) tick data is written/updated in *contract*/tickdata.csv .
Moreover, tick data is aggregated on daily granularity and written to *contract*/daily_agg.csv .
(contract dir will be of form BTC-28JUN-4000-C). Lastly, one
 
###features in csv files:
- daily_agg.csv: 
    - date_utc, ticker, strike, close (in BTC), IV (EOD), volume, contract (instrument name)

- tickdata.csv:
    - date_utc, amount, direction, indexPrice, instrument, iv, price, tickDirection, timeStamp, tradeId, tradeSeq