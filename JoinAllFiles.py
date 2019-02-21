import os
import pandas as pd

"""
Produce one csv file with all EOD data for all contracts
"""
df = pd.DataFrame()
# for root, dirs, files in os.walk("Data/"):
#     for file in files:
#         if file == "daily_agg.csv":
#             pd.read_csv()

root = "Data/"
subdirs = os.listdir(root)
all_data = pd.DataFrame()
for dir in subdirs:
    for file in [f for f in os.listdir(root+dir) if f == "daily_agg.csv"]:
        fpath = root+dir+"/"+file
        new = pd.read_csv(fpath)
        all_data = pd.concat([all_data, new], axis=0)
all_data = all_data.set_index('contract')
all_data.to_csv('all_data.csv')