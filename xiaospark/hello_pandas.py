#!/usr/bin/env python
# encoding: utf-8

"""
@description: pandas

@author: baoqiang
@time: 2018/7/27 下午4:22
"""

import pandas as pd
import numpy as np


def run():
    axis_sample2()


def axis_sample2():
    x = [[1, 2, 3], [4, 5, 6]]
    print(np.concatenate([x, x], axis=2))


def axis_sample1():
    df = pd.DataFrame([1, None, 2, None], [2, 3, 5, None], [None, 4, 6, None])

    df.fillna(method='ffill', inplace=True, axis=1)

    print(df)


if __name__ == '__main__':
    run()
