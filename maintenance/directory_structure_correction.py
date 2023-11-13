import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from tqdm import  tqdm
import json
import shutil

def find_day_layer(path):
    """
    Finds the layer of the directory that contains the folders with the files
    :param path:
    :return:
    """
    for entry in os.scandir(path):
        if entry.is_dir():
            return find_day_layer(entry.path)
        elif entry.is_file():
            return os.path.dirname(path)

def move_to_root(root, day_layer):
    """
    Moves all folders in a directory to the root of the directory
    :param root:
    :param day_layer:
    :return:
    """
    for folder in os.scandir(day_layer):
        # move the folder to root
        shutil.move(os.path.join(day_layer, folder), root)

def check_empty(dir):
    """
    Checks if a directory is empty
    :param dir:
    :return:
    """
    with os.scandir(dir) as scan:
        return not any(scan)  # Returns False when it finds the first entry

def delete_empty_folders(dir):
    """
    Deletes empty folders recursively from the bottom up
    :param dir:
    :return:
    """
    for entry in os.scandir(dir):
        if entry.is_dir():
            delete_empty_folders(entry.path)  # Recursively check subdirectories
            if check_empty(entry.path):  # Delete if it's empty
                os.rmdir(entry.path)


def restructure_hierarchy(root):
    """
    Moves folders that contain files but are buried in nested folders to the root of the directory.
    And some other stuff
    :param root:
    :return:
    """
    for entry in os.scandir(root):
        if entry.is_dir():
            day_layer = find_day_layer(os.path.join(root, entry.name))
            if day_layer == root:
                continue
            move_to_root(root, day_layer)
        else:
            return
    delete_empty_folders(root)
    dir_list = os.listdir(root)
    if len(dir_list) == 1 and dir_list[0] == os.path.basename(root):
        new_name = os.path.join(os.path.dirname(root), 'for_deletion')
        os.rename(root, new_name)
        shutil.move(os.path.join(new_name, dir_list[0]), os.path.dirname(root))
        os.rmdir(new_name)


# temp = r'Z:\Data\Twitter\temp'
# for file in os.listdir(temp):
# restructure_hierarchy(r'Z:\Data\Twitter\temp\2023-06-22')

dirs = [r'Z:\Data\Twitter\A\unzipped', r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
for dir in dirs:
    for file in tqdm(os.listdir(dir)):
        restructure_hierarchy(os.path.join(dir, file))





