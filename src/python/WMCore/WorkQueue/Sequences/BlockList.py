#!/usr/bin/env python
# encoding: utf-8
"""
BlockList.py

Created by Dave Evans on 2010-09-15.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest


class Block(dict):
    """
    _Block_

    Object representing a File Block
    """
    def __init__(self, **opts):
        dict.__init__(self)
        self.setdefault("name", None)
        self.setdefault("dataset", None)
        self.setdefault("dbsurl", None)
        self.setdefault("file_count", 0)
        self.setdefault("event_count", 0)
        self.setdefault("run_count", 0)
        self.setdefault("lumi_count", 0)
        self.update(opts)


class BlockList(dict):
    def __init__(self):
        dict.__init__(self)
        self['blocks'] = {}
        self.currentList = []

    def init(self, wqe):
        """
        _init_

        initialise from WQE
        """
        runs = wqe.document[u'data'].get(u'blocks', {})
        for k,v in  runs.items():
            self['blocks'][str(k)] = v

        self.currentList = self['blocks'].keys()
        self.currentList.sort()

    def append(self, unitOfData):
        """
        _append_

        Add a new block to this list
        """
        self['blocks'][unitOfData['name']] = unitOfData

    def size(self):
        return len(self['blocks'].keys())

    def start(self, initialSequence):
        """
        _start_

        Initialise for iteration from a parent sequence
        """
        pass


    def end(self, wqe):
        """
        _end_

        Add state to wqe provided
        """
        wqe.document[u'data'][u'blocks'] = self['blocks']

    def next(self):
        """
        _next_

        Block iterator
        """
        if len(self.currentList) == 0:
            raise StopIteration
        result = self.currentList.pop(0)
        blockData = self['blocks'].get(result)
        block = Block(**blockData)
        return block

    def __iter__(self):
        return self

class BlockListTests(unittest.TestCase):
  def setUp(self):
      pass

  def testA(self):

      class WQE(object):
             def __init__(self):
                 self.document = {}
                 self.document[u'data'] = {
                     u'blocks': {} }
      seq = BlockList()

      tmpWQE = WQE()
      for i in range(0,20):
          b = Block(name = "block%s" % i, dataset = "/prim/proc/tier")
          tmpWQE.document[u'data'][u'blocks'][b['name']] = b


      seq.init(tmpWQE)
      print seq.size()
      for x in seq:
          print x
      doneWQE = WQE()
      seq.end(doneWQE)
      print doneWQE.document[u'data']




if __name__ == '__main__':
	unittest.main()