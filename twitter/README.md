# Twitter Information Operations (IO) Dataset
# Script
The Python script `twitter-data.py` has been used to generate the summarized output data from the 40+GBs CSV dataset provided by Twitter on their website.
More information can be found in the original [Twitter announcement](https://blog.twitter.com/en_us/topics/company/2018/enabling-further-research-of-information-operations-on-twitter.html) from October 2018.

The script generates a `twitter-data.csv` file which contains
- the top hashtags
- top domain names
- top languages
- top countries - that were retrieved through reverse geocoding when available. In some cases, it only says `present` in the dataset which prevents to do further analysis
- number of Twitter interactions per tweet (likes, retweets, replies)

# Codebook
TBD