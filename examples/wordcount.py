#!/usr/bin/env python
# encoding: utf-8

"""
@description: 词频统计

@author: baoqiang
@time: 2018/7/18 上午11:36
"""
import luigi


class InputText(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('./data/%Y-%m-%d.txt'))


class WordCountTask(luigi.Task):
    task_namespace = 'examples'

    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [InputText(date) for date in self.date_interval.dates()]

    def output(self):
        return luigi.LocalTarget('./data/{}.output'.format(self.date_interval))

    def run(self):
        count_dic = {}

        for f in self.input():
            for line in f.open('r'):
                for word in line.strip().split():
                    count_dic[word] = count_dic.get(word, 0) + 1

        fw = self.output().open('w')
        for word, count in count_dic.items():
            fw.write('{}\t{}\n'.format(word, count))
        fw.close()


if __name__ == '__main__':
    luigi.run(['examples.WordCountTask', '--workers', '1', '--date-interval', '2018-07-18', '--local-scheduler'])
