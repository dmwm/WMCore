#! /usr/bin/env python
"""
Unit testing base class that turns on emulators
"""

from __future__ import (division, print_function)

import unittest

import mock

from WMQuality.Emulators.DBSClient.MockDbsApi import MockDbsApi


class EmulatedUnitTest(unittest.TestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.

    FIXME: For now only DBS is mocked
    """

    def setUp(self):
        """
        Start the various mocked versions and add cleanups in case of exceptions

        TODO: parameters to turn off emulators individually
        """

        self.dbsPatcher = mock.patch('dbs.apis.dbsClient.DbsApi', new=MockDbsApi)
        self.inUseDbsApi = self.dbsPatcher.start()
        self.addCleanup(self.dbsPatcher.stop)

        return
