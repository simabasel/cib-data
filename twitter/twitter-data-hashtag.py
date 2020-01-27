# We only keep the top 10,000 hashtags
import glob, os
import pandas as pd

hashtags_files = glob.glob(os.path.join('', "*tweets_hashtags.csv"))
for hashtags_file in hashtags_files:
    df = pd.read_csv(hashtags_file)
    new_name = hashtags_file.replace('_hashtags.csv', '_hashtags_top10000.csv')
    df.head(10000).to_csv(new_name)