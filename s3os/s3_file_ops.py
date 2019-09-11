#!/usr/bin/env python
# coding:UTF-8
# Date: 2019.01.11 (Fri)

""" For OS routines inside S3 repositories
"""

__version__ = '1.0.0' # Done for requirements control
__author__ = 'Kota Matsuo <kota.matsuo@agoop.co.jp>' # Just for future reference


import os
import re
import codecs
from io import StringIO

import numpy as np
import pandas as pd
import boto3
# import botocore

from .utils import mem_check
from . import file_ops


class S3os:
    def __init__(self, bucket_name):
        """For performing os module like routines inside S3 repositories.
    
        List of os methods supported:
            listdir, copy, remove, rename,
            download_dir, upload_dir,
            exists, create_if_not_exists, 
            save_dataframe, 
            read_all_csv_files_in_directory_as_one_df
        
        Example:       
            >>> S3OS = S3os(bucket_name)
            >>> list_of_file_paths = S3OS.listdir(dir_path) # same as os.listdir()
            >>> S3OS.copy(dir_path) # same as os.copy()
            
        Note:
            'Path' & 'key' are synonyms except 'key' is for files on S3.
        """
        try:
            # If on AWS Sagemaker
            from sagemaker import get_execution_role
            self.role = get_execution_role()
        else:
            pass
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(self.bucket_name)

    def listdir(self, dir_path, substring=None):
        """Same as os.listdir() except for S3 objects
        
        Get the list of all file names in 's3://{bucket}/{dir_path}'.
        """
        list_of_all_files = [this_obj.key for this_obj in self.bucket.objects.filter(Prefix=dir_path)]
        if substring != None:
            list_of_files = [f for f in list_of_all_files if substring in f]
            return list_of_files
        else:
            return list_of_all_files
    
    def copy(self, copy_this_key, to_here):
        """Same as os.copy() except for S3 objects
        """
        copy_source = {'Bucket': self.bucket_name,'Key': copy_this_key}
        self.s3.meta.client.copy(copy_source, self.bucket_name, to_here)
        return
    
    def remove(self, this_key):
        """Same as os.remove() except for S3 objects
        """
        obj = self.bucket.Object(this_key)
        response = obj.delete()
        return response

    def rename(self, old_key, new_key):
        """Same as os.rename() except for S3 objects
        """
        self.copy(old_key, new_key)
        self.delete(old_key)
        return
    
    def save_dataframe(self, df_to_save, file_path):
        """Same as pd.DataFrame.to_csv()
        E.g.
            file_path = 'data/test.csv'
        """
        csv_buffer = StringIO() # Use BytesIO for pickle, etc.
        df_to_save.to_csv(csv_buffer)
        self.bucket.Object(file_path).put(Body=csv_buffer.getvalue())
        return

    def download_dir(self, download_this_s3_dir, save_to_this_dir):
        paginator = self.client.get_paginator('list_objects')
        paginated_results = paginator.paginate(
            Bucket=self.bucket_name, 
            Delimiter=os.sep, 
            Prefix=download_this_s3_dir
        )
        for result in paginated_results:
            if result.get('CommonPrefixes') is not None:
                # Download recursively through all the subfolders
                for subdir in result.get('CommonPrefixes'):
                    self.download_dir(subdir.get('Prefix'), save_to_this_dir)
            if result.get('Contents') is not None:
                for file in result.get('Contents'):
                    this_file_path = file.get('Key')
                    this_full_file_path = os.path.join(save_to_this_dir, os.sep, this_file_path)
                    this_dir = os.path.dirname(this_full_file_path)
                    if not os.path.exists(this_dir):
                        os.makedirs(this_dir)
                    if this_file_path.endswith('/'):
                        continue
                    self.client.download_file(self.bucket_name, this_file_path, this_full_file_path)
        return

    def upload_dir(self, local_dir_path, s3_dir_path):
        """Upload a local directory to S3
        
        E.g.
            local_dir_path = '/home/ec2-user/SageMaker/project_x/data/'
            s3_dir_path = 'data/raw/'
        """
        
        # Get list of file paths (without folder paths)
        list_of_file_and_dir_paths = file_ops.get_list_of_file_paths_in_dir(local_dir_path, subfolders=True)
        list_of_file_paths = []
        list_of_file_save_path = []
        for this_path in list_of_file_and_dir_paths:
            this_basename = os.path.basename(this_path)
            if '.' in this_basename: # Exclude folders
                list_of_file_paths.append(this_path)
                
                this_s3_path = this_path.replace(local_dir_path, s3_dir_path)
                list_of_file_save_path.append(this_s3_path)
        
        for this_path, this_s3_path in zip(list_of_file_paths, list_of_file_save_path):
            self.client.upload_file(this_path, self.bucket_name, this_s3_path)
    
#         for root, dirs, files in os.walk(local_dir_path):
#             for file in files:
#                 file_path = os.path.join(root, file)
#                 self.client.upload_file(file_path, self.bucket_name, s3_dir_path + file)


    def exists(self, key_prefix, format_size=False):
        """Return the key's size if it exist, else None
        
        Same as os.path.exists()
        """
        
        response = self.client.list_objects_v2(
            Bucket = self.bucket_name,
            Prefix = key_prefix
        )
        
        # Regex to check if a path starts with the key
        re_prefix = re.compile(key_prefix)
        
        # Take the sum of all relevant files
        size = 0
        for obj in response.get('Contents', []):
            this_key = obj['Key']
            if re_prefix.match(this_key):
                size += obj['Size']
        
        # No files found with the key
        if size == 0:
            return
        
        if format_size:
            def _format_file_size(size):
                """Convert bytes to kB, MB, GB"""
                power = 2**10 #1024
                n = 0
                power_labels = {0: '', 1:'k', 2: 'M', 3:'G'}
                while size > power:
                    size /= power
                    n += 1
                formated_size = str(round(size, 2)) + ' ' + power_labels[n]+'B'
                return formated_size
            # Format total fize size from bytes to kB, MB, GB
            size = _format_file_size(size)
    
        if response.get('KeyCount', []) == 1000:
            # Check if it reached max key counts
            min_size = '>'+ str(size)
            return min_size
        else:
            return size

    def create_if_not_exists(self, key):
        """Creates the folder/file if it doesn't already exist.
        
        Similar to os.makedirs()
        # create_if_not_exists

        Args:
            key (str): For example "folder/folder/file.csv"

        """
        if self.exists(self.bucket_name, key) == None:
            self.client.put_object(Bucket=self.bucket_name, Key=key)
            mem_check('Following folder/file created: {}'.format(key))
        else:
            mem_check('Follwoing folder/file already exists: {}'.format(key))
        return

    def read_all_csv_files_in_directory_as_one_df(self, dir_path):
        all_df = []
        r_test = re.compile('[0-9]+')
        list_of_all_files = self.listdir(dir_path)
        n_files = len(list_of_all_files)
        for idx, file in enumerate(list_of_all_files):
            if idx % 50 == 0:
                mem_check('Read {}/{}'.format(idx, n_files))

            if ( file.endswith(".csv") ) & ( bool(r_test.search(file)) ):
                start_of_n = r_test.search(file).start()
                end_of_n   = r_test.search(file).end()
                n_patents = int(file[start_of_n:end_of_n])
                if n_patents == 0:
                    continue
                this_file_path = 's3://{}/{}'.format(self.bucket_name, file)
                this_df = pd.read_csv(encoding='utf-8', filepath_or_buffer=this_file_path, delimiter=',', low_memory=False)
#                 with codecs.open(this_file_path, "r", "utf-8", "ignore") as f:
#                     this_df = pd.read_table(f, delimiter=",", low_memory=False)
                all_df.append(this_df)
            else:
                pass
        df = pd.concat(all_df, axis=0)
        mem_check('Read all csv files in directory as one DataFrame')
        return df


#     def makedirs(name, mode=0o777, exist_ok=False):
#         """makedirs(name [, mode=0o777][, exist_ok=False])
#         Super-mkdir; create a leaf directory and all intermediate ones.  Works like
#         mkdir, except that any intermediate path segment (not just the rightmost)
#         will be created if it does not exist. If the target directory already
#         exists, raise an OSError if exist_ok is False. Otherwise no exception is
#         raised.  This is recursive.
#         """
#         head, tail = path.split(name)
#         if not tail:
#             head, tail = path.split(head)
#         if head and tail and not path.exists(head):
#             try:
#                 makedirs(head, exist_ok=exist_ok)
#             except FileExistsError:
#                 # Defeats race condition when another thread created the path
#                 pass
#             cdir = curdir
#             if isinstance(tail, bytes):
#                 cdir = bytes(curdir, 'ASCII')
#             if tail == cdir:           # xxx/newdir/. exists if xxx/newdir exists
#                 return
#         try:
#             mkdir(name, mode)
#         except OSError:
#             # Cannot rely on checking for EEXIST, since the operating system
#             # could give priority to other errors like EACCES or EROFS
#             if not exist_ok or not path.isdir(name):
#     raise


#     def mkdir(self, mode=0o777, parents=False, exist_ok=False):
#             """
#             Create a new directory at this given path.
#             """
#             if self._closed:
#                 self._raise_closed()
#             try:
#                 self._accessor.mkdir(self, mode)
#             except FileNotFoundError:
#                 if not parents or self.parent == self:
#                     raise
#                 self.parent.mkdir(parents=True, exist_ok=True)
#                 self.mkdir(mode, parents=False, exist_ok=exist_ok)
#             except OSError:
#                 # Cannot rely on checking for EEXIST, since the operating system
#                 # could give priority to other errors like EACCES or EROFS
#                 if not exist_ok or not self.is_dir():
#     raise