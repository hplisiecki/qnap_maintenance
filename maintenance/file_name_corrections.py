import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from tqdm import  tqdm
import json
cap = 300000
dirs = [r'Z:\Data\Twitter\A\ZIPPED', r'Z:\Data\Twitter\B1\ZIPPED', r'Z:\Data\Twitter\B2\ZIPPED', r'Z:\Data\Twitter\C1\ZIPPED', r'Z:\Data\Twitter\C2\ZIPPED', r'Z:\Data\Twitter\D\ZIPPED', r'Z:\Data\Twitter\E\ZIPPED']
for dir in dirs:
    numbered = False
    letter = dir.split('\\')[-2]
    if len(letter) == 2:
        numbered = True

    for file in os.listdir(dir):
        if numbered and file[:2] != letter:
            new_file = letter + '_' + file
            # change file name
            os.rename(os.path.join(dir, file), os.path.join(dir, new_file))

        elif not numbered and file[0] != letter:
            new_file = letter + '_' + file
            # change file name
            os.rename(os.path.join(dir, file), os.path.join(dir, new_file))
