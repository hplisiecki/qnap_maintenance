import os
from tqdm import tqdm

dir_to_placeholder = r'Z:\Data\Twitter\PLACEHOLDER\ZIPPED'
dir_from = r'Z:\Data\Twitter\temp'
letters = ['A', 'B1', 'B2', 'C1', 'C2', 'D', 'E']
file_lists = {letter: os.listdir(dir_to_placeholder.replace('PLACEHOLDER', letter)) for letter in letters}
for entry in tqdm(os.scandir(dir_from)):
    letter = entry.name.split('_')[0]
    dir_to = dir_to_placeholder.replace('PLACEHOLDER', letter)
    # check if its there
    if entry.is_dir():
        continue
    elif entry.name not in file_lists[letter]:
        # move it
        os.rename(os.path.join(dir_from, entry.name), os.path.join(dir_to, entry.name))
