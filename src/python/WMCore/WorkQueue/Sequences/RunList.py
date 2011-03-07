#!/usr/bin/env python
# encoding: utf-8
"""
RunList.py

Created by Dave Evans on 2010-09-09.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.DataStructs.Run import Run

class RunList(dict):
    def __init__(self):
        dict.__init__(self)
        self['runs'] = {}
        self.currentList = []

    def size(self):
        """
        how big is this run range?
        """
        return len(self['runs'].keys())

    def init(self, wqe):
        """
        _init_

        initialise this sequence from the wqe it belongs to

        """
        runs = wqe.document[u'data'].get(u'runs', {})
        for k,v in  runs.items():
            self['runs'][int(k)] = v

        self.currentList = self['runs'].keys()
        self.currentList.sort()

    def append(self, unitOfData):
        """
        _append_

        Add a Run to this sequence
        """
        self['runs'][int(unitOfData.run)] = unitOfData

    def start(self, oldSequence):
        """
        _start_

        prep for iteration given the work queue element provided

        """
        pass

    def end(self, wqe):
        """
        _end_

        finish iteration
        """
        wqe.document[u'data'][u'runs'] = self['runs']

    def next(self):
        """
        _next_

        implement iterator that produces sub elements

        """
        if len(self.currentList) == 0:
            raise StopIteration
        result = self.currentList.pop(0)
        runData = self['runs'].get(result)
        run = Run(result)
        return run


    def __iter__(self):
        """make this object be an iterator"""
        return self



class RunListTests(unittest.TestCase):
    def setUp(self):
        pass

    def testA(self):

        class WQE(object):
            def __init__(self):
                self.document = {}
                self.document[u'data'] = {
                    u'runs': {} }

        seq = RunList()
        tmpWQE = WQE()
        for i in range(0,20):
            tmpWQE.document[u'data'][u'runs'][i] = Run(i)



        seq.init(tmpWQE)
        print seq.size()
        for x in seq:
            print x
        doneWQE = WQE()
        seq.end(doneWQE)
        print doneWQE.document[u'data']


if __name__ == '__main__':
    unittest.main()