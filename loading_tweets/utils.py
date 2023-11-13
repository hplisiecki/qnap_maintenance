import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import os
from tqdm import tqdm

MAIN_DIR = r'Z:\Data\Twitter'

class TweetLoader():
    """
    Loads tweets from the parquet files
    """
    def __init__(self, MAIN_DIR):
        self.MAIN_DIR = MAIN_DIR
        self.letters = ['A', 'B1', 'B2', 'C1', 'C2', 'D', 'E']
        self.stems_folder = 'stems_new'
        self.clean_folder = 'arrow_new'

    def load_file(self, dir):
        table = pq.read_table(dir)
        df = table.to_pandas()
        return df

    def check_keywords(self, keywords, letter = 'all', column = 'stemmed', save = False, save_dir = None):
        """
        Iterates through the tweets in given letter and collects tweets containing the keywords

        :param keywords: list of keywords
        :param letter: letters to check, or 'all' for all letters
        :param column: column to check for keywords (default: 'stemmed') can also be text
        :param save: if True, saves the results to a csv file; if False, returns a dataframe
        :param save_dir: directory to save the results to
        :return: dataframe of tweets containing the keywords
        """
        pattern = '|'.join(keywords)
        df_list = []
        if letter == 'all':
            for letter in self.letters:
                print(letter)
                tweets_dir = os.path.join(self.MAIN_DIR, letter, self.stems_folder)
                files = os.listdir(tweets_dir)
                files = [file for file in files if 'users' not in file]
                for file in tqdm(files):
                    df = self.load_file(os.path.join(tweets_dir, file))
                    mask = df[column].str.contains(pattern, na=False,case=False)
                    df = df[mask]
                    df_list.append(df)

        else:
            print(letter)
            tweets_dir = os.path.join(self.MAIN_DIR, letter, self.stems_folder)
            files = os.listdir(tweets_dir)
            files = [file for file in files if 'users' not in file]
            for file in tqdm(files):
                df = self.load_file(os.path.join(tweets_dir, file))
                mask = df[column].str.contains(pattern, na=False, case=False)
                df = df[mask]
                df_list.append(df)
                break

        concatenated_df = pd.concat(df_list)
        if save:
            pq.write_table(pa.Table.from_pandas(concatenated_df), save_dir)
            return

        return concatenated_df

    def get_authors(self, user_ids, letter = 'all', save = False, save_dir = None):
        """
        Iterates through the tweets in given letter and collects user rows
        :param user_ids: list of user ids
        :param letter: letters to check, or 'all' for all letters
        :param save: if True, saves the results to a csv file; if False, returns a dataframe
        :param save_dir: directory to save the results to
        :return: dataframe of users containing the keywords
        """
        df_list = []
        if letter == 'all' or isinstance(letter, list):
            for letter in self.letters:
                print(letter)
                tweets_dir = os.path.join(self.MAIN_DIR, letter, self.clean_folder)
                files = os.listdir(tweets_dir)
                files = [file for file in files if 'users' in file]
                for file in tqdm(files):
                    df = self.load_file(os.path.join(tweets_dir, file))
                    df = df[df['id'].isin(user_ids)]
                    df_list.append(df)
        else:
            print(letter)
            tweets_dir = os.path.join(self.MAIN_DIR, letter, self.clean_folder)
            files = os.listdir(tweets_dir)
            files = [file for file in files if 'users' in file]
            for file in tqdm(files):
                df = self.load_file(os.path.join(tweets_dir, file))
                df = df[df['id'].isin(user_ids)]
                df_list.append(df)

        concatenated_df = pd.concat(df_list)
        if save:
            pq.write_table(pa.Table.from_pandas(concatenated_df), save_dir)
            return

        return concatenated_df


LOADER = TweetLoader(MAIN_DIR)

# Example usage:
# keywords = ['polska']
# df = LOADER.check_keywords(keywords, letter = 'A', save = False, save_dir = None)







df = LOADER.load_file(r'Z:\Data\Twitter\A\arrow_new\users_2022-03-22.parquet')