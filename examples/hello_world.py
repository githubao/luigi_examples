#!/usr/bin/env python
# encoding: utf-8

"""
@description: luigi hello-world

@author: baoqiang
@time: 2018/7/18 上午11:29
"""

import luigi


class HelloWorldTask(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print('{} says: hello luigi!'.format(self.__class__.__name__))


if __name__ == '__main__':
    luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler'])
