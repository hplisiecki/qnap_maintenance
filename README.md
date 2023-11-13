# Qnap Twitter Maintenance

This Readme file contains information about the scripts used to maintain the Twitter database on the Qnap server.

---
## Main Scripts for Maintenance
This section relates to scripts from the "maintenance" folder
1. unzip.py - Unzips files in all letter folders
2. to_arrow.py - Converts all files in all letter folders to arrow format
3. main_stemming - Applies stemming to Tweets from all letter folders and saves them in another directory
4. checking_continuity.py - Checks what files are missing from all letter folders


### Additional utility maintenance scripts
1. moving_zipped.py - Used at some point to move zipped files to proper location
2. multi_threding_stemming.py - Old version of stemming script
3. file_name_corrections.py - Used in the past to harmonize the names of the files
4. directory_structure_correction.py - Corrects directory structure when repetitively embedded folders are added to the database

---

## Loading Tweets
This section relates to scripts from the "loading_tweets" folder

1. load_tweets.py - Houses the TweetLoader Class, which can be used to seearch for tweets that contain specific keywords, and load or save them, as well as to seearch for specific user data information

### Example Usage for keyword search
```python
from load_tweets import TweetLoader

# Initialize the TweetLoader with the main directory
loader = TweetLoader(MAIN_DIR)

# Define the keywords to search for
keywords = ['polska', 'ukraina', 'rosja']

# Search for tweets containing these keywords in letter 'B1'
df = loader.check_keywords(keywords, letter='B1')

# Display the first few results
print(df.head())
```
### Example Usage for user search
```python
# Initialize the TweetLoader with the main directory
loader = TweetLoader(MAIN_DIR)

# Define a list of user IDs
user_ids = [12345, 67890, 54321]

# Fetch user information for these IDs in letter 'C1'
user_df = loader.get_authors(user_ids, letter='C1')

# Display the first few results
print(user_df.head())
```

---

## Qnap Twitter Directory Structure:

/Data/Twitter:
```
.
├── 4processing - previously used to dump data for processing, now empty
├── A
│   ├── arrow - deprecated arrow files
│   ├── arrow_new - UP TO DATE arrow files of users and Tweets in format 'tweets_{date}.parquet' and 'users_{date}.parquet'
│   ├── stems_new - UP TO DATE arrow files of stemmed Tweets in format 'tweets_{date}.parquet' 
│   ├── stems_no_rt - deprecated stemmed non retweets files
│   ├── stems_rt - deprecated stemmed retweets files
│   ├── unzipped - contains unzipped files
│   ├── ZIPPED - contains zipped files
├── B1
... the same subdirectories as A
├── B2
... the same subdirectories as A
├── C1
... the same subdirectories as A
├── C2
... the same subdirectories as A
├── D
... the same subdirectories as A
├── data - used to dump additional data for processing, contains only D zip files
├── dezinformation - contains profiles of accounts from the list of potential desinformators
├── E
... the same subdirectories as A
├── logs - contains logs, potentially from the Twitter scraping process
├── new_data - potentially used in the past to dump additional data, now empty
├── Piotrek - data for Piotr, used to compute Twitter trends in the past
├── temp - used ion the past to dump additional data, now contains some C1, C2, E, some logs, and pozostale-tweety.zip (?)
├── tweets_from_users - Additional tweets from desinformation accounts, related to the "dezinformation" folder
├── Opis zbierania danych.txt - Description of the data collection process
.
```

---

## Tweets File Column Structure:

1. 'text' - This column contains the actual text of the tweet. It includes the content posted by the user, excluding media attachments.

3. 'id' - The unique identifier for the tweet. This is a numeric value that Twitter assigns to each tweet, which can be used to reference the specific tweet.

3. 'author' - This field stores the username or handle of the Twitter user who posted the tweet. It helps in identifying the source of the tweet.

4. 'created' - The creation date and time of the tweet. It is usually in a standard date-time format, indicating when the tweet was posted.

5. 'replies_count' - A count of the number of replies to the tweet. This indicates how many times other users have responded directly to this tweet.

6. 'likes_count' - This column shows the number of likes the tweet has received. It's an indicator of the tweet's popularity or user engagement.

7. 'quotes_count' - The number of times the tweet has been quoted by other users. Quoting a tweet means other users have shared this tweet with their commentary.

8. 'impression_count' - This represents the number of times the tweet was seen. This can be a key metric for measuring the reach or impact of the tweet.

9. 'possibly_sensitive' - A boolean flag indicating whether the tweet contains content that users might find sensitive (e.g., violence, adult content). True means it may contain sensitive content.

10. 'conversation_id' - The identifier for the conversation to which the tweet belongs. This helps in linking tweets that are part of the same conversation or thread.

11. ONLY IN STEMMED FILES 'RT' - whether a tweet is a RT or not 

12. ONLY IN STEMMED FILES 'stemmed' - stemmed Tweet content
---

## User File Column Structure:

1. 'id' - The unique identifier for the Twitter user. This numeric value is assigned by Twitter to each user.
2. 'username' - The Twitter handle or username of the user. It is unique to each Twitter account.
3. 'avatar_url' - URL of the user's profile picture or avatar. This link leads to the image file.
4. 'created' - The date and time when the user's Twitter account was created.
5. 'description' - The self-description provided by the user in their Twitter profile.
6. 'name' - The name of the user as displayed on their Twitter profile. This may differ from the username.
7. 'location' - The location provided by the user in their Twitter profile. It is a user-inputted field and may not always represent an actual location.
8. 'followers' - The number of followers the user has. This indicates the user's audience size.
9. 'following' - The count of how many other Twitter users this account is following.
10. 'tweets' - The total number of tweets (including retweets) posted by the user.
11. 'lists' - The number of public lists that the user is included in. This is a measure of the user's influence or reach.
