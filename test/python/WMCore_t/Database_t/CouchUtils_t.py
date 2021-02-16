#!/usr/bin/env python
# encoding: utf-8
"""
CouchUtils_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import object

import unittest

import WMCore.Database.CouchUtils as CouchUtils
from WMQuality.TestInitCouchApp import TestInitCouchApp


class CouchUtils_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchutils", "GroupUser", "ACDC")

    def tearDown(self):
        self.testInit.tearDownCouch()

    def testA(self):
        """object driven connection via initialiseCouch method"""

        class Thingy(object):
            """misc object with couch access attrs"""

            def __init__(self):
                self.couchdb = None
                self.database = None
                self.url = None

            @CouchUtils.connectToCouch
            def __call__(self):
                return True

        couchThingy = Thingy()

        # test throws with everything None
        self.assertRaises(CouchUtils.CouchConnectionError, CouchUtils.initialiseCouch, couchThingy)
        couchThingy.url = self.testInit.couchUrl
        self.assertRaises(CouchUtils.CouchConnectionError, CouchUtils.initialiseCouch, couchThingy)
        couchThingy.database = self.testInit.couchDbName

        try:
            CouchUtils.initialiseCouch(couchThingy)
        except Exception as ex:
            msg = "Error initialising couch client for test object:\n %s " % str(ex)
            self.fail(msg)

        self.assertIsNotNone(couchThingy.couchdb)
        # test decorator on already connected object
        try:
            couchThingy()
        except Exception as ex:
            msg = "Error invoking connectToCouch decorator:\n %s" % str(ex)
            self.fail(msg)

        newCouchThingy = Thingy()
        newCouchThingy.database = self.testInit.couchDbName
        newCouchThingy.url = self.testInit.couchUrl
        # 2nd call will make sure full connection is called
        try:
            newCouchThingy()
        except Exception as ex:
            msg = "Error invoking connectToCouch decorator:\n %s" % str(ex)
            self.fail(msg)
        self.assertIsNotNone(newCouchThingy)

    def testB(self):
        """check requirement tests"""

        class Thingy(dict):
            """test object with required attrs"""

            def __init__(self):
                super(Thingy, self).__init__()
                self.collection = "NotNone"
                self.owner = "NotNone"
                self['fileset_id'] = "NotNone"
                self['owner_id'] = "NotNone"

            @CouchUtils.requireCollection
            def call1(self):
                return True

            @CouchUtils.requireOwner
            def call4(self):
                return True

        thingy = Thingy()

        try:
            thingy.call1()
        except Exception as ex:
            msg = "Failure in requireCollection decorator: %s" % str(ex)
            self.fail(msg)
        try:
            thingy.call4()
        except Exception as ex:
            msg = "Failure in requireOwner decorator: %s" % str(ex)
            self.fail(msg)

        # now screw it up
        thingy.collection = None
        thingy.owner = None

        self.assertRaises(RuntimeError, thingy.call1)
        self.assertRaises(RuntimeError, thingy.call4)


if __name__ == '__main__':
    unittest.main()
