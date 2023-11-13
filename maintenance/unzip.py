import os
import zipfile
from tqdm import tqdm


dirs = [r'Z:\Data\Twitter\A\ZIPPED', r'Z:\Data\Twitter\B1\ZIPPED', r'Z:\Data\Twitter\B2\ZIPPED', r'Z:\Data\Twitter\C1\ZIPPED', r'Z:\Data\Twitter\C2\ZIPPED', r'Z:\Data\Twitter\D\ZIPPED', r'Z:\Data\Twitter\E\ZIPPED']
destinations = [r'Z:\Data\Twitter\A\unzipped', r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
dir_to_placeholder = r'Z:\Data\Twitter\PLACEHOLDER\unzipped'
letters = ['A', 'B1', 'B2', 'C1', 'C2', 'D', 'E']
file_lists = {letter: os.listdir(dir_to_placeholder.replace('PLACEHOLDER', letter)) for letter in letters}

for dir, destination in zip(dirs, destinations):
    letter = dir.split('\\')[-2]
    for file in tqdm(os.listdir(dir)):
        if ('retrospective' not in file) and ('collection' not in file):
            new_name = file.replace('.zip', '').replace(f'{letter}_', '')
            if new_name not in file_lists[letter]:
                try:
                    # extract and move to destination saved as new_name
                    with zipfile.ZipFile(os.path.join(dir, file), 'r') as zip_ref:
                        zip_ref.extractall(os.path.join(destination, new_name))
                except Exception as e:
                    print(f"Failed to unzip {file} due to {str(e)}")





