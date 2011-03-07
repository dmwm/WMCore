#!/usr/bin/env python
# encoding: utf-8
"""
RunRange.py

Created by Dave Evans on 2010-09-09.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os

from WMCore.DataStructs.Run import Run

class RunRange(dict):


    def __init__(self):
        dict.__init__(self)
        self.setdefault("first_run", None)
        self.setdefault("increment_run", None)
        self.setdefault("last_run", None)
        self.currentRun = None


    def size(self):
        """
        how big is this run range?
        """
        lastMinusFirst = self['last_run'] - self['first_run']
        totalRuns = lastMinusFirst/self['increment_run']
        return totalRuns

    def append(self, unitOfData):
        """
        _append_

        Add the given unit of data to this sequence
        In this case, we expect a Run instance
        """
        if self['first_run'] == None:
            self['first_run'] = unitOfData.run
        if self['last_run'] == None:
            self['last_run'] = unitOfData.run
        if unitOfData.run < self['first_run']:
            self['first_run'] = unitOfData.run
        if unitOfData.run > self['last_run']:
            self['last_run'] = unitOfData.run

    def start(self, sequence):
        """
        _start_

        initialise from parent sequence for iteration, copy settings
        not state here
        """
        self['increment_run'] = sequence['increment_run']

    def init(self, wqe):
        """
        _init_

        initialise this sequence from the wqe that contains it

        """
        self['first_run'] = int(wqe.element_data.get(u'first_run'))
        self['last_run']  = int(wqe.element_data.get(u'last_run'))
        self['increment_run'] = int(wqe.element_data.get(u'increment_run'))

        currentRun = wqe.element_state.get(u'current_run', None)
        if currentRun != None:
            self.currentRun = int(currentRun)
        else:
            self.currentRun = self['first_run']


    def end(self, wqe):
        """
        _end_

        finish iteration, persist state if needed
        """
        wqe.document[u'data'][u'first_run'] = self['first_run']
        wqe.document[u'data'][u'last_run'] = self['last_run']
        wqe.document[u'data'][u'increment_run'] = self['increment_run']


    def next(self):
        """
        _next_

        implement iterator that produces sub elements containing run ranges

        """
        if self.currentRun > self['last_run']:
            raise StopIteration
        result = Run(self.currentRun)
        self.currentRun += self['increment_run']
        return  result

    def __iter__(self):
        return self


def main():


    seq = RunRange()
    seq['first_run'] = 0
    seq['last_run'] = 10
    seq['increment_run'] = 1
    seq.currentRun = 1


    print seq.size()
    seq.append(Run(21))
    print seq.size()
    for x in seq:
        print x


if __name__ == '__main__':
    main()
