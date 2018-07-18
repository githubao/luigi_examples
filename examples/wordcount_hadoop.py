#!/usr/bin/env python
# encoding: utf-8

"""
@description: hadoop数据源的词频统计

@author: baoqiang
@time: 2018/7/18 上午11:53
"""
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs


class InputText(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('/pgc/baoqiang/examples/%Y-%m-%d.txt'))


class WordCountHadoopTask(luigi.contrib.hadoop.JobTask):
    task_namespaces = 'examples'

    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [InputText(date) for date in self.date_interval.dates()]

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/pgc/baoqiang/examples/{}.output'.format(self.date_interval),
                                             format=luigi.contrib.hdfs.Plain)

    def jobconfs(self):
        jcs = super(luigi.contrib.hadoop.JobTask, self).jobconfs()
        jcs.append('mapred.compress.map.output=false')
        jcs.append('mapred.output.compress=false')
        jcs.append('mapred.output.fileoutputformat.compress=false')
        return jcs

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    luigi.run(main_task_cls=WordCountHadoopTask, local_scheduler=True,
              cmdline_args=['--workers', '1', '--date-interval', '2018-07-18'])
