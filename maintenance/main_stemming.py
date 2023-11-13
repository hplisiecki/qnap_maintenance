import os
from pyMorfologik import Morfologik
from pyMorfologik.parsing import ListParser
import pickle
import multiprocessing
import pyarrow as pa
import pyarrow.parquet as pq
import time
import string

# Load lemmatizer dictionary
with open(r'D:\PycharmProjects\Ukraina\data/lemmatizer_dictionary.pickle', 'rb') as handle:
    lema_dict = pickle.load(handle)

# Load stopwords
with open(r'D:\PycharmProjects\Ukraina\data/stopwords.txt', 'r') as f:
    stopwords = f.read().splitlines()

# Convert stopwords to a set for faster lookup
stopwords_set = set(stopwords)


parser = ListParser()
stemmer = Morfologik()

translator = str.maketrans('', '', string.punctuation)


def stem(sentence):
    # Remove 'RT' prefix
    if sentence.startswith('RT'):
        sentence = sentence[2:]

    # Remove punctuation using string translate
    sentence = sentence.translate(translator)

    # Tokenize and filter words
    words = sentence.split()
    words = [word for word in words if word not in stopwords_set and '@' not in word and 'http' not in word]

    # Get stem of words
    morf = stemmer.stem([' '.join(words).lower()], parser)

    stemmed_words = []
    for i in morf:
        if i[0] in lema_dict:
            stemmed_words.append(lema_dict[i[0]])
        else:
            stemmed_words.append(list(i[1].keys())[0] if i[1] else i[0])

    return ' '.join(stemmed_words)

def safe_split(text):
    parts = text.split(': ')
    return parts[1] if len(parts) > 1 else parts[0]

def process_file(file, dir, destination):
    table = pq.read_table(os.path.join(dir, file))
    df = table.to_pandas()
    df['RT'] = df['text'].str.startswith('RT')
    df['text'] = df['text'].where(~df['RT'], df['text'].apply(safe_split))

    texts_dates = [(str(idx), str(text)) for idx, text in
                   zip(df['id'], df['text'])]

    texts_set = df['text'].unique()

    with multiprocessing.Pool(16) as pool:
        stemmed_texts = pool.map(stem, texts_set)

    dictionary = dict(zip(texts_set, stemmed_texts))
    df['stemmed'] = [dictionary[text] for _, text in texts_dates]

    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(destination, file))


if __name__ == '__main__':
    dirs = [r'Z:\Data\Twitter\A\arrow_new', r'Z:\Data\Twitter\E\arrow_new', r'Z:\Data\Twitter\B1\arrow_new', r'Z:\Data\Twitter\B2\arrow_new', r'Z:\Data\Twitter\C1\arrow_new', r'Z:\Data\Twitter\C2\arrow_new']
    destinations = [r'Z:\Data\Twitter\A\stems_new', r'Z:\Data\Twitter\E\stems_new', r'Z:\Data\Twitter\B1\stems_new', r'Z:\Data\Twitter\B2\stems_new', r'Z:\Data\Twitter\C1\stems_new', r'Z:\Data\Twitter\C2\stems_new']

    for dir, destination in zip(dirs, destinations):
        dir_list = os.listdir(dir)
        # sort alphabetically
        dir_list.sort()
        dest_list = os.listdir(destination)

        for file in dir_list:
            if 'tweets' in file and file not in dest_list:
                print(file)
                t1 = time.time()
                process_file(file, dir, destination)
                t2 = time.time()
                print(f"Processed {file} in {(t2 - t1) / 60} minutes")
