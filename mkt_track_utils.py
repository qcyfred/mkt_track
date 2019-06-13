# coding: utf-8
import numpy as np


# 把np.float转为普通float
def trans_number_type(x):
    if np.isnan(x):
        x = None
    else:
        x = float(x)

    return x
