#!/usr/bin/env python
# coding: UTF-8
# Author: Kota Matsuo
# Date: 2018.10.19 (Fri)

import os
import time
import logging
import psutil
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def mem_check(message='no message', logging_level='info'):
    ''' checks current usage of memory and also outputs message'''
    try:
        rss = psutil.Process(os.getpid()).memory_info().rss  # rss is in bytes
        memory = int(rss / 1024**2)  # convert bytes to megabytes
        if logging_level=='info':
            logging.info('[{message}] RAM usage: {memory} MB'.format(
                message=message, memory=memory))
        elif logging_level=='debug':
            logging.debug('[{message}] RAM usage: {memory} MB'.format(
                message=message, memory=memory))

    except Exception as e:
        logging.exception(repr(e) + ' while checking memory usage')
    finally:
        time.sleep(0)