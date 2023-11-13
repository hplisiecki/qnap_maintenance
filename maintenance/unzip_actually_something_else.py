import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from tqdm import  tqdm
import json
cap = 300000
dirs = [r'Z:\Data\Twitter\A\unzipped', r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
destinations = [r'Z:\Data\Twitter\A\arrow', r'Z:\Data\Twitter\B1\arrow', r'Z:\Data\Twitter\B2\arrow', r'Z:\Data\Twitter\C1\arrow', r'Z:\Data\Twitter\C2\arrow', r'Z:\Data\Twitter\D\arrow', r'Z:\Data\Twitter\E\arrow']
for dir, destination in zip(dirs, destinations):
    for file in os.listdir(dir):
        if (file != "retrospective 02.03_XX.XX") and ('data' not in file):
            if (file + '.parquet') not in os.listdir(destination):
                temp_dir = os.path.join(dir, file)
                texts = []
                id = []
                author = []
                created = []
                for subfile in tqdm(os.listdir(temp_dir)):
                    if (subfile.endswith('.json')):
                        with open(os.path.join(temp_dir, subfile), 'r', encoding = "utf-8") as f:
                            data = json.load(f)
                        try:
                            for tweet in data:
                                texts.append(tweet['text'])
                                id.append(tweet['id'])
                                author.append(tweet['author_id'])
                                created.append(tweet['created_at'])
                        except:
                            continue
                pydict = {'text': texts, 'id': id, 'author': author, 'created': created}
                table = pa.Table.from_pydict(pydict)
                print(len(table))
                pq.write_table(table, os.path.join(destination, f'{file}.parquet'))