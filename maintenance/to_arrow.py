import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from tqdm import  tqdm
import json
cap = 300000
dirs = [r'Z:\Data\Twitter\A\unzipped', r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
destinations = [r'Z:\Data\Twitter\A\arrow_new', r'Z:\Data\Twitter\B1\arrow_new', r'Z:\Data\Twitter\B2\arrow_new', r'Z:\Data\Twitter\C1\arrow_new', r'Z:\Data\Twitter\C2\arrow_new', r'Z:\Data\Twitter\D\arrow_new', r'Z:\Data\Twitter\E\arrow_new']
dirs = [ r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
destinations = [r'Z:\Data\Twitter\B1\arrow_new', r'Z:\Data\Twitter\B2\arrow_new', r'Z:\Data\Twitter\C1\arrow_new', r'Z:\Data\Twitter\C2\arrow_new', r'Z:\Data\Twitter\D\arrow_new', r'Z:\Data\Twitter\E\arrow_new']


replace = False
breaking = False

def get_file_size(filename):
    size = os.path.getsize(filename)  # This gives the size in bytes
    return size

for dir, destination in zip(dirs, destinations):
    for file in os.listdir(dir):
        if file != "retrospective 02.03_XX.XX":
            if ('tweets_' + file + '.parquet') not in os.listdir(destination) or replace:
                print(file)
                temp_dir = os.path.join(dir, file)

                ### FOR TWEETS
                texts = []
                id = []
                author = []
                created = []
                replies_count = []
                likes_count = []
                quotes_count = []
                impression_count = []
                possibly_sensitive = []
                conversation_id = []

                ### FOR USERS
                user_id = []
                user_username = []
                user_avatar_url = []
                user_created = []
                user_description = []
                user_name = []
                user_location = []
                user_followers = []
                user_following = []
                user_tweets = []
                user_lists = []
                counter = 0

                for subfile in tqdm(os.listdir(temp_dir)):
                    if get_file_size(os.path.join(temp_dir, subfile)) < 10:
                        continue
                    if (subfile.endswith('.json')):
                        counter += 1
                        try:
                            with open(os.path.join(temp_dir, subfile), 'r', encoding = "utf-8") as f:
                                data = json.load(f)
                        except:
                            print('Failed to load json:', os.path.join(temp_dir, subfile))
                            breaking = True
                            break

                        if 'users' in subfile:
                            users = data['users']
                            try:
                                data = data['tweets']
                            except:
                                print('Failed to find tweets:', os.path.join(temp_dir, subfile))
                                continue
                            try:
                                for user in users:
                                    user_id.append(user['id'])
                                    user_username.append(user['username'])
                                    user_avatar_url.append(user['profile_image_url'])
                                    user_created.append(user['created_at'])
                                    try:
                                        user_description.append(user['description'])
                                    except:
                                        user_description.append(None)
                                    try:
                                        user_name.append(user['name'])
                                    except:
                                        user_name.append(None)
                                    try:
                                        user_location.append(user['location'])
                                    except:
                                        user_location.append(None)
                                    user_followers.append(user['public_metrics']['followers_count'])
                                    user_following.append(user['public_metrics']['following_count'])
                                    user_tweets.append(user['public_metrics']['tweet_count'])
                                    try:
                                        user_lists.append(user['public_metrics']['listed_count'])
                                    except:
                                        user_lists.append(None)
                            except:
                                print('Failed to append to users:', os.path.join(temp_dir, subfile))
                        try:
                            for tweet in data:
                                texts.append(tweet['text'])
                                id.append(tweet['id'])
                                author.append(tweet['author_id'])
                                created.append(tweet['created_at'])
                                replies_count.append(tweet['public_metrics']['reply_count'])
                                likes_count.append(tweet['public_metrics']['like_count'])
                                quotes_count.append(tweet['public_metrics']['quote_count'])
                                try:
                                    impression_count.append(tweet['public_metrics']['impression_count'])
                                except:
                                    impression_count.append(None)
                                possibly_sensitive.append(tweet['possibly_sensitive'])
                                conversation_id.append(tweet['conversation_id'])
                        except:
                            print('Failed to append to tweets:', os.path.join(temp_dir, subfile))
                if breaking:
                    break
                if counter > 0:
                    pydict_users = {'id': user_id, 'username': user_username, 'avatar_url': user_avatar_url,
                                    'created': user_created, 'description': user_description, 'name': user_name,
                                    'location': user_location, 'followers': user_followers,
                                    'following': user_following, 'tweets': user_tweets, 'lists': user_lists}
                    table = pa.Table.from_pydict(pydict_users)
                    pq.write_table(table, os.path.join(destination, f'users_{file}.parquet'))
                    del users, pydict_users, table, user_id, user_username, user_avatar_url, user_created, user_description, user_name, user_location, user_followers, user_following, user_tweets, user_lists


                    pydict_data = {'text': texts, 'id': id, 'author': author, 'created': created, 'replies_count': replies_count, 'likes_count': likes_count, 'quotes_count': quotes_count, 'impression_count': impression_count, 'possibly_sensitive': possibly_sensitive, 'conversation_id': conversation_id}
                    table = pa.Table.from_pydict(pydict_data)
                    pq.write_table(table, os.path.join(destination, f'tweets_{file}.parquet'))

    if breaking:
        break

with open(os.path.join(temp_dir, subfile), 'r', encoding="utf-8") as f:
    users = json.load(f)

with open(os.path.join(temp_dir, subfile.replace('users', 'data')), 'r', encoding="utf-8") as f:
    data = json.load(f)


user_id_users = [user['id'] for user in users['users']]
tweets_id_users = list(set([tweet['author_id'] for tweet in users['tweets']]))
tweets_id_data = list(set([tweet['author_id'] for tweet in data]))

remaining = [user_id for user_id in tweets_id_users if user_id not in user_id_users]


file = r'Z:\Data\Twitter\A\unzipped\2022-03-02\users_1499058338910322700.json'
with open(file, 'r', encoding="utf-8") as f:
    users = json.load(f)


file = r'Z:\Data\Twitter\A\unzipped\2022-03-02\data_1499058338910322700.json'
with open(file, 'r', encoding="utf-8") as f:
    data = json.load(f)

tweet_ids = [tweet['id'] for tweet in data]
user_tweet_id = [tweet['id'] for tweet in users['tweets']]

remaining = [id for id in tweet_ids if id not in user_tweet_id]

remaining = [tweet for tweet in data if tweet['id'] in remaining]

tweet_conv_ids = list(set([tweet['conversation_id'] for tweet in data]))
user_tweets_conv_ids = list(set([tweet['conversation_id'] for tweet in users['tweets']]))


user_users = list(set([user['author_id'] for user in users['tweets']]))
data_users = list(set([tweet['author_id'] for tweet in data]))



for tweet in data:
    a = tweet['text']
    a = tweet['id']
    a = tweet['author_id']
    a = tweet['created_at']
    a = tweet['public_metrics']['reply_count']
    a = tweet['public_metrics']['like_count']
    a = tweet['public_metrics']['quote_count']
    try:
        a = tweet['public_metrics']['impression_count']
    except:
        a = None
    a = tweet['possibly_sensitive']
    a = tweet['conversation_id']
    a = tweet['geo']





filename = r'Z:\Data\Twitter\A\unzipped\2022-03-02\data_1499043853910818819.json'