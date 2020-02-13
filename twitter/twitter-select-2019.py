
import pandas as pd
import numpy as np

import os.path
import glob, os

from datetime import date, datetime

Y2019 = pd.Timestamp(2019, 1, 1)

filename = ""
outfilename = ""

dfObj = pd.DataFrame()

print("[+++] %s" % (filename))
df_chunk = pd.read_csv(filename, 
    usecols=["user_display_name","user_screen_name","user_reported_location","user_profile_description", "tweet_text", "tweet_language", "tweet_time"],
    parse_dates=["tweet_time"])

mask = (df_chunk.tweet_time >= Y2019)

df_chunk.loc[mask].sort_values(by='tweet_time').to_csv(outfilename, index=False, encoding='utf-8-sig')

