#!/usr/bin/env python
# encoding: utf-8

"""
@description: luigi调度pyspark

@author: baoqiang
@time: 2018/7/18 下午2:26
"""
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext


class InlineSparkWordCountTask(PySparkTask):
    driver_memory = '2g'
    executor_memory = '3g'

    def input(self):
        return luigi.LocalTarget('./data/2018-07-18.txt')

    def output(self):
        return luigi.LocalTarget('./data/2018-07-18.output.spark')

    def main(self, sc, *args):
        sc.textFile(self.input().path) \
            .flatMap(lambda line: line.split()) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .saveAsTextFile(self.output().path)


if __name__ == '__main__':
    luigi.run(main_task_cls=InlineSparkWordCountTask, local_scheduler=True)
