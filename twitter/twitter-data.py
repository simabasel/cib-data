import pandas as pd
import numpy as np
import reverse_geocoder as rg
from collections import Counter
import os.path
import glob, os
import tldextract

from datetime import date, datetime
import json

def get_json(df):
    """ Small function to serialise DataFrame dates as 'YYYY-MM-DD' in JSON """

    def convert_timestamp(item_date_object):
        if isinstance(item_date_object, (pd.Period)):
            return item_date_object.strftime("%Y-%m")
        else:
            return item_date_object

    dict_ = df.to_dict()
    dict_ = {convert_timestamp(k):v for k, v in dict_.items()}

    return json.dumps(dict_)

def tidy_split(df, column, sep=',', keep=False, domain=False):
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            # Cleaning hashtags
            value = value.replace('[', '')
            value = value.replace(' ', '')
            value = value.replace("'" , '')
            value = value.replace(']', '')
            if domain:
                if value.startswith('http'):
                    tld_data = tldextract.extract(value)
                    value = tld_data.domain.lower() + "." + tld_data.suffix.lower()
                else:
                    value = ''
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df

def getCountry(latitude, longitude):
    isValid = False

    if type(latitude) == str:
        if latitude != 'absent' and latitude != 'present' and len(latitude) > 0:
            isValid = True
    else:
        if latitude > 0:
            isValid = True

    # In certain dataset we only have absent/present instead of the data
    if isValid:
        # print(latitude)
        coordinates = (latitude, longitude)
        # print(coordinates)
        result = rg.search(coordinates, 1)
        # print(result)
        return result[0]['cc']

    return ''

def get_users_information(identifier, dir_path, filepath):
    users_information_size = os.path.getsize(dir_path + '/' + filepath)
    df_chunk = pd.read_csv(dir_path + '/' + filepath, chunksize=500000, usecols=["follower_count", "following_count", "account_language"])
    total_follower_count = 0
    total_following_count = 0
    users_count = 0

    users_info = {}

    df_account_languages = pd.DataFrame(columns = ['account_languages', 'count'])
    df_account_languages = df_account_languages.set_index('account_languages')

    # Each chunk is in df format
    for chunk in df_chunk:
        # perform data filtering 
        # chunk_filter = chunk_preprocessing(chunk)

        users_count += chunk.shape[0]
        total_follower_count += chunk['follower_count'].sum()
        total_following_count += chunk['following_count'].sum()

        value_counts = chunk['account_language'].value_counts()
        df_value_counts = value_counts.rename_axis('account_languages').to_frame('count')
        df_account_languages = df_account_languages.append(df_value_counts)
        df_account_languages = df_account_languages.groupby(df_account_languages.index).sum()

    account_languages = ",".join(df_account_languages.index.to_list())
    print("[***][%s]\nusers_count: %d\ntotal_follower_count: %d\ntotal_following_count: %d\naccount_languages: %s\n" % (filepath, users_count, total_follower_count, total_following_count, account_languages))

    df_account_languages = df_account_languages.sort_values(by =['count'], ascending=False)
    # print(df_account_languages)

    dst_languages_file = dir_path + '_' + filepath.replace('_csv_hashed', '_account_languages')
    # if dst_languages_file:
    #    df_account_languages.to_csv(dst_languages_file)

    users_info['users_filename'] = filepath
    users_info['accounts'] = users_count
    users_info['accounts_reported'] = users_count
    # users_info['account_languages'] = account_languages
    users_info['account_languages'] = df_account_languages.dropna()["count"].to_json()
    # users_info['account_languages_filename'] = dst_languages_file
    users_info['total_follower_count'] = total_follower_count
    users_info['total_following_count'] = total_following_count
    users_info['users_information_size'] = users_information_size

    # Add empty columns
    users_info['entry_date'] = date.today().strftime("%d/%m/%Y")
    users_info['release_date'] = dir_path
    users_info['attributed_country'] = ''
    users_info['affiliation'] = ''
    users_info['target_countries'] = ''
    users_info['tweet_media_size'] = ''
    users_info['description'] = ''
    users_info['urls'] = ''

    return users_info

def get_tweets_information(identifier,
                           dir_path,
                           files):
    total_quote_count = 0
    total_reply_count = 0
    total_like_count = 0
    total_retweet_count = 0
    tweets_count = 0

    tweets_info = {}

    df_src_cc = pd.DataFrame(columns = ['tweet_from_cc', 'count'])
    df_src_cc = df_src_cc.set_index('tweet_from_cc')

    df_hashtags = pd.DataFrame(columns = ['hashtags', 'count'])
    df_hashtags = df_hashtags.set_index('hashtags')

    df_domains = pd.DataFrame(columns = ['domains', 'count'])
    df_domains = df_domains.set_index('domains')

    df_tweet_languages = pd.DataFrame(columns = ['tweet_languages', 'count'])
    df_tweet_languages = df_tweet_languages.set_index('tweet_languages')

    df_timeline = pd.DataFrame(columns = ['tweet_time', 'count'])
    df_timeline = df_timeline.set_index('tweet_time')

    df_tweet_client_name = pd.DataFrame(columns = ['tweet_client_name', 'count'])
    df_tweet_client_name = df_tweet_client_name.set_index('tweet_client_name')

    tweet_time = []

    filepath = files[0]
    filepath = filepath.replace('_1.csv', '.csv')
    filepath = filepath.replace('_part1.csv', '.csv')

    tweet_information_size = 0

    for filename in files:
        tweet_information_size += os.path.getsize(dir_path + '/' + filename)

        print("[+++] %s" % (filename))
        df_chunk = pd.read_csv(dir_path + '/' + filename, chunksize=500000, 
            usecols=["tweet_client_name", "latitude","longitude","quote_count","reply_count","like_count","retweet_count","hashtags", "urls", "tweet_language", "tweet_time"],
            parse_dates=["tweet_time"])

        for chunk in df_chunk:
            total_quote_count += chunk['quote_count'].sum()
            total_reply_count += chunk['reply_count'].sum()
            total_like_count += chunk['like_count'].sum()
            total_retweet_count += chunk['retweet_count'].sum()
            tweets_count += chunk.shape[0]

            chunk['src_country'] = chunk.apply(lambda row : getCountry(row['latitude'], row['longitude']), axis = 1)

            value_counts = chunk['src_country'].value_counts()
            df_value_counts = value_counts.rename_axis('tweet_from_cc').to_frame('count')
            df_src_cc = df_src_cc.append(df_value_counts)
            df_src_cc = df_src_cc.groupby(df_src_cc.index).sum()

            tags = tidy_split(chunk, 'hashtags', sep=',')
            tags = tags['hashtags']
            # print(tags)

            value_counts = tags.value_counts()
            df_value_counts = value_counts.rename_axis('hashtags').to_frame('count')
            df_hashtags = df_hashtags.append(df_value_counts)
            df_hashtags = df_hashtags.groupby(df_hashtags.index).sum()

            domains = tidy_split(chunk, 'urls', sep=',', domain=True)
            domains = domains['urls']
            # print(tags)

            value_counts = domains.value_counts()
            df_value_counts = value_counts.rename_axis('domains').to_frame('count')
            df_domains = df_domains.append(df_value_counts)
            df_domains = df_domains.groupby(df_domains.index).sum()

            ## Tweet languages
            value_counts = chunk['tweet_language'].value_counts()
            df_value_counts = value_counts.rename_axis('tweet_languages').to_frame('count')
            df_tweet_languages = df_tweet_languages.append(df_value_counts)
            df_tweet_languages = df_tweet_languages.groupby(df_tweet_languages.index).sum()

            # Client name
            value_counts = chunk['tweet_client_name'].value_counts()
            df_value_counts = value_counts.rename_axis('tweet_client_name').to_frame('count')
            df_tweet_client_name = df_tweet_client_name.append(df_value_counts)
            df_tweet_client_name = df_tweet_client_name.groupby(df_tweet_client_name.index).sum()

            # Timeline information
            df_date_count = chunk['tweet_time'].groupby(chunk.tweet_time.dt.to_period("M")).agg('count')
            df_date_count = df_date_count.rename_axis('tweet_time').to_frame('count')
            df_timeline = df_timeline.append(df_date_count)
            df_timeline = df_timeline.groupby(df_timeline.index).sum()

            tweet_time.append(chunk["tweet_time"].min())
            tweet_time.append(chunk["tweet_time"].max())

    print("[***][%s]\ntweets_count: %d\ntotal_quote_count: %d\ntotal_reply_count: %d\ntotal_like_count: %d\ntotal_retweet_count: %d\n" % (filepath, tweets_count, total_quote_count, total_reply_count, total_like_count, total_retweet_count))

    filepath = filepath.split('/')[-1]
    # dst_countries_file = dir_path + '_' + filepath.replace('_csv_hashed', '_countries')
    dst_hashtags_file = dir_path + '_' + filepath.replace('_csv_hashed', '_hashtags')
    dst_domains_file = dir_path + '_' + filepath.replace('_csv_hashed', '_domains')
    # dst_languages_file = dir_path + '_' + filepath.replace('_csv_hashed', '_languages')
    dst_clients_file = dir_path + '_' + filepath.replace('_csv_hashed', '_clients')

    df_src_cc = df_src_cc[df_src_cc.index != '']
    df_src_cc = df_src_cc.sort_values(by =['count'], ascending=False)
    # print(df_src_cc)
    # if dst_countries_file:
    #    df_src_cc.to_csv(dst_countries_file, encoding='utf-8-sig')

    df_hashtags = df_hashtags[df_hashtags.index != '']
    df_hashtags = df_hashtags.sort_values(by =['count'], ascending=False)
    # print(df_hashtags)
    if dst_hashtags_file:
        # Save top 10K hashtags
        df_hashtags.head(10000).to_csv(dst_hashtags_file, encoding='utf-8-sig')

    df_domains = df_domains[df_domains.index != '']
    df_domains = df_domains.sort_values(by =['count'], ascending=False)
    # print(df_hashtags)
    if dst_domains_file:
        # Save top 10K domains
        df_domains.head(10000).to_csv(dst_domains_file, encoding='utf-8-sig')

    df_tweet_client_name = df_tweet_client_name[df_tweet_client_name.index != '']
    df_tweet_client_name = df_tweet_client_name.sort_values(by =['count'], ascending=False)

    # print(df_hashtags)
    if dst_clients_file:
        df_tweet_client_name.to_csv(dst_clients_file, encoding='utf-8-sig')

    df_tweet_languages = df_tweet_languages[df_tweet_languages.index != '']
    df_tweet_languages = df_tweet_languages.sort_values(by =['count'], ascending=False)
    # print(df_hashtags)
    # if dst_languages_file:
    #    df_tweet_languages.to_csv(dst_languages_file, encoding='utf-8-sig')

    df_tweet_time = pd.DataFrame(tweet_time)

    tweets_info['tweets_filename'] = filepath
    tweets_info['total_quote_count'] = total_quote_count
    tweets_info['total_reply_count'] = total_reply_count
    tweets_info['total_like_count'] = total_like_count
    tweets_info['total_retweet_count'] = total_retweet_count
    tweets_info['tweets_count'] = tweets_count
    df_not_null = df_hashtags[df_hashtags.index != '']
    # tweets_info['hashtags'] = ",".join(df_not_null.head(25).dropna().index.to_list())
    tweets_info['hashtags'] = df_not_null.head(25).dropna()["count"].to_json()
    tweets_info['hashtags_filename'] = dst_hashtags_file
    # tweets_info['domains'] = ",".join(df_not_null.head(25).dropna().index.to_list())
    tweets_info['domains'] = df_domains.head(25).dropna()["count"].to_json()
    tweets_info['domains_filename'] = dst_domains_file

    # tweets_info['tweets_from'] = ",".join(df_not_null.dropna().index.to_list())
    tweets_info['georeverse_codes'] = df_src_cc.dropna()["count"].to_json()
    # tweets_info['tweets_from_filename'] = dst_countries_file

    # df_not_null = df_tweet_languages[df_tweet_languages.index != '']
    tweets_info['tweet_languages'] = df_tweet_languages.dropna()["count"].to_json()
    # tweets_info['tweet_languages_filename'] = dst_languages_file

    tweets_info['tweet_client_names'] = df_tweet_client_name.head(25).dropna()["count"].to_json()
    # tweets_info['tweet_client_names_filename'] = dst_clients_file

    tweets_info['first_tweet'] = df_tweet_time[0].min()
    tweets_info['last_tweet'] = df_tweet_time[0].max()

    tweets_info['activity_timeline'] = get_json(df_timeline["count"])

    tweets_info['tweets_information_size'] = tweet_information_size

    return tweets_info

## Process files
def processOct2018():
    if os.path.isfile('2018_10_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2018_10', r'ira_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2018_10', [r'ira_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2018_10', r'iranian_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2018_10', [r'iranian_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2018_10_twitter-data.csv', index=False, encoding='utf-8-sig')

def processJan2019():
    if os.path.isfile('2019_01_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2019_01', r'bangladesh_201901_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_01', [r'bangladesh_linked_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_01', r'iran_201901_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_01', [r'iran_201901_1_tweets_csv_hashed_1.csv',
        r'iran_201901_1_tweets_csv_hashed_2.csv',
        r'iran_201901_1_tweets_csv_hashed_3.csv',
        r'iran_201901_1_tweets_csv_hashed_4.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_01', r'russia_201901_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_01', [r'russian_linked_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_01', r'venezuela_201901_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_01', [r'venezuela_201901_1_tweets_csv_hashed_1.csv',
        r'venezuela_201901_1_tweets_csv_hashed_2.csv',
        r'venezuela_201901_1_tweets_csv_hashed_3.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_01', r'venezuela_201901_2_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_01', [r'venezuela_linked_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2019_01_twitter-data.csv', index=False, encoding='utf-8-sig')

def processJune2019():
    if os.path.isfile('2019_06_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2019_06', r'venezuela_201906_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'venezuela_201906_1_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_06', r'russia_201906_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'russia_201906_1_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_06', r'iran_201906_3_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'iran_201906_3_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_06', r'iran_201906_2_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'iran_201906_2_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_06', r'iran_201906_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'iran_201906_1_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_06', r'catalonia_201906_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_06', [r'catalonia_201906_1_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2019_06_twitter-data.csv', index=False, encoding='utf-8-sig')

def processAug2019():
    if os.path.isfile('2019_08_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2019_08', r'china_082019_1_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_08', [r'china_082019_1_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_08', r'china_082019_2_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_08', [r'china_082019_2_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2019_08_twitter-data.csv', index=False, encoding='utf-8-sig')

def processSept2019():
    if os.path.isfile('2019_09_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2019_09', r'china_082019_3_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'china_082019_3_tweets_csv_hashed_part1.csv',
        r'china_082019_3_tweets_csv_hashed_part2.csv',
        r'china_082019_3_tweets_csv_hashed_part3.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_09', r'ecuador_082019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'ecuador_082019_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_09', r'egypt_uae_082019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'egypt_uae_082019_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_09', r'saudi_arabia_082019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'saudi_arabia_082019_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_09', r'spain_082019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'spain_082019_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", '2019_09', r'uae_082019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_09', [r'uae_082019_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2019_09_twitter-data.csv', index=False, encoding='utf-8-sig')

def processDec2019():
    if os.path.isfile('2019_12_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2019_12', r'saudi_arabia_112019_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2019_12', [r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_1.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_2.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_3.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_4.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_5.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_6.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_7.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_8.csv',
        r'saudi_arabia_112019_tweets_csv_hashed/saudi_arabia_112019_tweets_csv_hashed_9.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2019_12_twitter-data.csv', index=False, encoding='utf-8-sig')


def processMarch2020():
    if os.path.isfile('2020_03_twitter-data.csv'):
        return

    rows_list = []
    users_info = get_users_information("", '2020_03', r'032020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", '2020_03', [r'032020_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv('2020_03_twitter-data.csv', index=False, encoding='utf-8-sig')

def processApril2020(month_id):
    if os.path.isfile(month_id + '_twitter-data.csv'):
        return

    rows_list = []
    
    users_info = get_users_information("", month_id, r'egypt_022020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", month_id, [
        r'hashed_2020_04_egypt_022020_egypt_022020_tweets_csv_hashed/egypt_022020_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)
    
    users_info = get_users_information("", month_id, r'honduras_022020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", month_id, [
        r'honduras_022020_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)
    
    users_info = get_users_information("", month_id, r'indonesia_022020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", month_id, [
        r'indonesia_022020_tweets_csv_hashed.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", month_id, r'sa_eg_ae_022020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", month_id, [
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_01.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_02.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_03.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_04.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_05.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_06.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_07.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_08.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_09.csv',
        r'sa_eg_ae_022020_tweets_csv_hashed/sa_eg_ae_022020_tweets_csv_hashed_10.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    users_info = get_users_information("", month_id, r'serbia_022020_users_csv_hashed.csv')
    tweets_info = get_tweets_information("", month_id, [
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_01.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_02.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_03.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_04.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_05.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_06.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_07.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_08.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_09.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_10.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_11.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_12.csv',
        r'serbia_022020_tweets_csv_hashed/serbia_022020_tweets_csv_hashed_13.csv'])
    users_info.update(tweets_info)
    rows_list.append(users_info)

    df_global = pd.DataFrame(rows_list)
    df_global.to_csv(month_id + '_twitter-data.csv', index=False, encoding='utf-8-sig')


print("[!] Processing the Twitter released in October 2018...")
# processOct2018()
print("[!] Processing the Twitter released in January 2019...")
# processJan2019()
print("[!] Processing the Twitter released in June 2019...")
# processJune2019()
print("[!] Processing the Twitter released in August 2019...")
# processAug2019()
print("[!] Processing the Twitter released in September 2019...")
# processSept2019()
print("[!] Processing the Twitter released in December 2019...")
# processDec2019()
print("[!] Processing the Twitter released in March 2020...")
processMarch2020()
print("[!] Processing the Twitter released in April 2020...")
processApril2020('2020_04')

df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('', "*twitter-data.csv"))))
df.to_csv('twitter-data.csv', index=False, encoding='utf-8-sig')