#!/usr/bin/env python
# encoding: utf-8

"""
@description: spark的入门操作

@author: baoqiang
@time: 2018/7/20 下午3:46
"""

from __future__ import division, print_function
from pyspark import SparkContext, Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext, functions as F


def transform():
    sc = SparkContext()
    # sc.setLogLevel('INFO')

    # python类型变成rdd
    intRDD = sc.parallelize([3, 1, 2, 5, 5])
    intRDD2 = sc.parallelize([[1, 2], [3, 4], [5, 6]])
    stringRDD = sc.parallelize(['Apple', 'Orange', 'Grape', 'Banana', 'Apple'])

    # 转化rdd到我们需要的类型
    print(intRDD.collect())
    print(stringRDD.collect())

    # map
    print(intRDD.map(lambda x: x + 1).collect())
    print(intRDD2.flatMap(lambda x: x).collect())

    # filter
    print(intRDD.filter(lambda x: x > 3).collect())

    # distinct
    print(intRDD.distinct().collect())

    # random-split
    for i in range(3):
        print(', '.join([str(rdd.collect()) for rdd in intRDD.randomSplit([0.4, 0.6])]))

    # groupby
    kvs = intRDD.groupBy(lambda x: x % 2).collect()
    print({k: sorted(v) for k, v in kvs})

    # union intersection subtract
    intRDD2 = sc.parallelize([5, 6])
    print(intRDD.union(intRDD2).collect())


def reduce0():
    sc = SparkContext()
    int_rdd = sc.parallelize([3, 1, 2, 5, 5])

    # 取数据
    print(int_rdd.first())
    print(int_rdd.take(2))
    print(int_rdd.takeOrdered(1))
    print(int_rdd.takeOrdered(1, lambda x: -x))

    # 统计数据
    print(int_rdd.max())
    print(int_rdd.count())
    print(int_rdd.stdev())
    print(int_rdd.mean())
    print(int_rdd.stats().asDict())


def mapred():
    sc = SparkContext()
    kvRdd = sc.parallelize([(3, 4), (3, 6), (5, 6), (1, 2)])

    # 获取keys或者values
    print(kvRdd.keys().collect())
    print(kvRdd.values().collect())

    # 通过元组的第一个或者第二个元素取值
    print(kvRdd.filter(lambda x: x[0] < 5).collect())
    print(kvRdd.filter(lambda x: x[1] < 5).collect())

    # 针对键或值对应的操作
    print(kvRdd.sortByKey(False).collect())
    kvRdd.mapValues(lambda x: x * x).foreach(print)

    # 根据键的常见归并操作 combineByKey
    print(kvRdd.reduceByKey(lambda x, y: x + y).collect())

    kvs = kvRdd.groupByKey().collect()
    print({k: sorted(v) for k, v in kvs})

    print(kvRdd.foldByKey(0, lambda x, y: x + y).collect())


def rdd_df():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    scheme = StructType([
        StructField('name', StringType()),
        StructField('grade', IntegerType()),
        StructField('score', DoubleType()),
    ])

    rdd = sc.parallelize([('a', 3, 80.0), ('b', 3, 70.0), ('c', 5, 60.0), ('d', 1, 85.1)])

    df = sqlContext.createDataFrame(rdd, scheme)

    # df.foreach(print)

    # df2 = df.rdd.keyBy(lambda x: x['grade']).groupByKey().toDF()
    df2 = df.rdd.keyBy(lambda x: x['name']).mapValues(lambda x: int(x['score'])).toDF()
    df2.foreach(print)


def tmp():
    print(', '.join(str(i) for i in [1, 2, 3]))


def run():
    transform()
    # reduce0()
    # mapred()
    # rdd_df()


if __name__ == '__main__':
    run()
