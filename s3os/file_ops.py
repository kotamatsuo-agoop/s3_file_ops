#!/usr/bin/env python
# coding:UTF-8
# Author: Kota Matsuo
# Date: 2018.12.01 (Sat)

import os
import re
import glob
import codecs
import numpy as np
import pandas as pd
from . import list_ops


def change_permission(this_path, allow_read=True, allow_write=True):
    """Change file_folder permission.
    http://www.macinstruct.com/node/415
    """
    
    if not isinstance(allow_read, bool):
        raise AttributeError('the "allow_read" variable must be either True or False.')
    if not isinstance(allow_write, bool):
        raise AttributeError('the "allow_write" variable must be either True or False.')
    
    # Octal permission notations (Owner, Group, Everyone)
    if allow_read & allow_write:
        octal_permission = 0o770
    elif allow_read:
        octal_permission = 0o550
    elif allow_write:
        octal_permission = 0o330
    else: # Only allow execution
        octal_permission = 0o110
        
    os.chmod(this_path, octal_permission)
    return


def get_list_of_subdir_in_dir(directory):
    """Get list of all subfolders (including the parent folder itself)"""
    list_of_all_dirs = []
    for root, dirs, files in os.walk(directory):
        if not re.search('/$', root):
            root += os.sep # Add '/' to the end of root
        if '.ipynb_checkpoints' not in root:
            list_of_all_dirs.append(root)
    return list_of_all_dirs

def get_list_of_file_paths_in_dir(directory, substrings=None, subfolders=False, regex=False):
    """Yield all file paths under the directory.

    Example:
        'User/project/file.txt' like format

    Args:
        directory (str): Directory path.
        substrings (str|list): Character sequence or regular expression.
            If character sequence, an example would be 'projectA' or ['projectA','.csv'].
            If regex, an example would be r'.csv$'.
        subfolders (bool): If True, recursively goes through all subfolders.
        regex (bool): Is the substring regex or not.
            If True, assumes the substring is a regular expression.
            If False, treats the substring as a literal string.

    Returns:
        type: Description of returned object.

    """

    if subfolders == True:
        # Look through all subfolders too
        list_subdir = get_list_of_subdir_in_dir(directory)
        for this_subdir in list_subdir:
            yield from get_list_of_file_paths_in_dir(
                directory=this_subdir, 
                substrings=substrings, 
                subfolders=False, # Already considered
                regex=regex
            )

    # ⭐️ Get the file paths and names
    list_of_file_paths = glob.glob(os.path.join(directory,'*'))

    if (regex == True) & isinstance(substrings, list):
        raise Exception('If you want to match regex, the substrings should be of str type instead of list')
    if (regex == True) & isinstance(substrings, str):
        for f in list_of_file_paths:
            if re.search(substrings, os.path.basename(f)):
                yield f
    if (regex == False) & (substrings != None):
        # Make sure the substring is of list-type
        if isinstance(substrings, str):
            substrings = list(substrings.split(' '))
        for f in list_of_file_paths:
            if all(this_substring.upper() in f.upper() for this_substring in substrings):
                yield f
    if (regex == False) & (substrings == None):
        for f in list_of_file_paths:
            yield f

            
def get_file_names_from_paths(list_of_file_paths):
    """E.g. 'User/project/file.txt' --> 'file.txt' """
    if list_ops.is_list_like(list_of_file_paths):
        list_of_file_names = [os.path.basename(this_path) for this_path in list_of_file_paths]
        return list_of_file_names
    else:
        file_name = os.path.basename(list_of_file_paths)
        return file_name


def get_dir_names_from_paths(list_of_file_paths):
    """E.g. 'User/project/file.txt' --> 'User/project/' """
    if list_ops.is_list_like(list_of_file_paths):
        list_of_dir_names = list(set([os.path.dirname(this_path)+os.sep for this_path in list_of_file_paths]))
        return list_of_dir_names
    else:
        dir_name = os.path.dirname(this_path) + os.sep
        return dir_name


def read_all_csv_files_in_directory_as_one_df(folder_path, substrings='.csv', subfolders=False):   
    # Get all file paths in this folder/subfolders
    list_of_file_paths = get_list_of_file_paths_in_dir(folder_path, substrings, subfolders)
    
    all_df = []
    for this_file_path in list_of_file_paths:
        this_file_name = get_file_names_from_paths(this_file_path)
        with codecs.open(this_file_path, "r", "utf-8", "ignore") as f:
            this_df = pd.read_table(f, delimiter=",", low_memory=False)
        all_df.append(this_df)

    df = pd.concat(all_df)
    return df


def delete(this_file):
    if os.path.exists(this_file):
        os.remove(this_file)
    else:
        print("The file does not exist ({})".format(this_file))
    return