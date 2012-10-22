#!/usr/bin/env python

"""
URLFetcher

A unittest for seeing if we can pull code from a URL using the URLFetcher
Written by someone who has no idea what URLFetcher is supposed to do.
"""
import os
import re
import os.path
import unittest

from WMCore.WMSpec import WMTask
from WMCore.WMSpec import WMStep
from WMCore.WMSpec.Steps.Fetchers.URLFetcher import URLFetcher
from WMQuality.TestInitCouchApp              import TestInitCouchApp as TestInit

class URLFetcherTest(unittest.TestCase):
    """
    Main test for the URLFetcher

    """

    def setUp(self):
        """
        Basic setUp

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testDir = self.testInit.generateWorkDir()

        return

    def tearDown(self):
        """
        Basic tearDown

        """
        self.testInit.delWorkDir()

        return


    def createTask(self, fileURL):
        """
        _createTask_

        Create a test task that includes the
        fileURL
        """

        task = WMTask.makeWMTask("testTask")
        task.makeStep("step1")
        task.makeStep("step2")

        for t in task.steps().nodeIterator():
            t = WMStep.WMStepHelper(t)
            os.mkdir('%s/%s' % (self.testDir, t.name()))
            t.data.sandbox.section_('file0')
            t.data.sandbox.file0.src = fileURL
        return task


    def testA_BasicFunction(self):
        """
        _BasicFunction_

        Test and see if we can retrieve a basic file

        URL should not have http:// prefix in it
        """
        url     = 'cmsweb.cern.ch'
        task    = self.createTask(fileURL = 'http://%s' % url)
        fetcher = URLFetcher()
        fetcher.setWorkingDirectory(workingDir = self.testDir)
        fetcher(wmTask = task)

        f = open(os.path.join(self.testDir, 'step2', url))
        content = f.read()
        f.close()

        for x in ['html', 'CMS']:
            self.assertNotEqual( content.find(x), -1 )


if __name__ == "__main__":
    unittest.main()
