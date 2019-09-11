#!/usr/bin/env python
# coding:UTF-8
# Author: Kota Matsuo
# Date: 2018.12.01 (Sat)

import numpy as np
import pandas as pd

def is_list_like(variable):
    list_like = (
        list, tuple, 
        np.ndarray, pd.Series
        )
    if isinstance(variable, list_like):
        return True
    else:
        return False
