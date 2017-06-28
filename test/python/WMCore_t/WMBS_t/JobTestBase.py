#!/usr/bin/env python
"""
_JobTestBase

Base class for tests of WMBS job class.
"""
from __future__ import absolute_import, division, print_function

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit


class JobTestBase(unittest.TestCase):
    """
    Base class for tests of WMBS job class (Job_t and JobWorkUnit_t)
    """

    def __init__(self, *args, **kwargs):
        """
        Define some class instance variables we'll fill later
        """
        super(JobTestBase, self).__init__(*args, **kwargs)

        self.testFileA = None
        self.testFileB = None
        self.testWorkflow = None
        self.testJob = None

        return

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationNew = self.daoFactory(classname="Locations.New")
        locationNew.execute(siteName="test.site.ch", pnn="T2_CH_CERN")
        locationNew.execute(siteName="test2.site.ch", pnn="T2_CH_CERN")

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """

        self.testInit.clearDatabase()

    @staticmethod
    def createTestJob(subscriptionType="Merge"):
        """
        _createTestJob_

        Create a test job with two files as input.  This will also create the
        appropriate workflow, jobgroup and subscription.
        """

        testWorkflow = Workflow(spec=makeUUID(), owner="Simon", name=makeUUID(), task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset, workflow=testWorkflow, type=subscriptionType)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileB.addRun(Run(1, *[46]))
        testFileA.create()
        testFileB.create()

        testJob = Job(name=makeUUID(), files=[testFileA, testFileB])
        testJob["couch_record"] = "somecouchrecord"
        testJob["location"] = "test.site.ch"
        testJob.create(group=testJobGroup)
        testJob.associateFiles()

        return testJob

    def createSingleJobWorkflow(self):
        """
        Create a workflow with one jobs and two files and store the results in instance variables
        """

        self.testWorkflow = Workflow(spec="spec.xml", owner="Simon", name="wf001", task="Test")
        self.testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset, workflow=self.testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        self.testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        self.testFileA.addRun(Run(1, *[45]))
        self.testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        self.testFileB.addRun(Run(1, *[46]))
        self.testFileA.create()
        self.testFileB.create()

        self.testJob = Job(name="TestJob", files=[self.testFileA, self.testFileB])

        self.testJob.create(group=testJobGroup)
        self.testJob.associateFiles()
