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
## Master dataset
| Field                       | Description  |
|-----------------------------|--------------|
| entry_date                  |              |
| release_date                | October 2018 |
| attributed_country          | E.g Russia   |
| affiliation                 | E.g. Internet Research Agency |
| target_countries            |              |
| accounts                    | Number of unique accounts in the datasets |
| tweet_information_size      | In bytes. Computed from all the tweets CSV files (uncompressed) |
| users_information_size      | In bytes. Computed from all the tweets CSV files (uncompressed) |
| tweet_media_size            | Manual entry. (Bytes) |
| first_tweet                 |              |
| last_tweet                  |              |
| hashtags                    | Top hashtags |
| hashtags_filename           | Name of the file containing the hashtag information (generated via python script) |
| domains                     | Top domains  |
| domains_filename            |              |
| description                 |              |
| georeverse_codes            | Extract country names from (latitude geo-located latitude, if available longitude) |
| tweets_from_filename        |              |
| activity_timeline           |              |
| tweet_client_names          |              |
| tweet_client_names_filename |              |
| total_follower_count        | The sum of all the counts. (generated via a python script) |
| total_following_count       | The sum of all the counts. (generated via a python script) |
| total_quote_count           |              |
| total_reply_count           |              |
| total_like_count            |              |
| total_retweet_count         |              |
| tweet_count                 |              |
| tweet_languages             |              |
| tweet_languages_filename    |              |
| account_languages           | All the languages separated by a comma from account_language field. (generated via a python script) |
| account_languages_filename  | Separate file with the count from each languages |
| url                         | Source. E.g https://blog.twitter.com/en_us/topics/company/2018/enabling-further-research-of-information-operations-on-twitter.html |

## Hashtags/Domains/
| Field                       | Description  |
|-----------------------------|--------------|
| hashtag/domain              |              |
| total_count                 |              |