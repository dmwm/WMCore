#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""


# test file skeletons.


import os
import unittest
import threading
import time

#FIXME: need to be migrated to new wmcore jobspec stuff.
from ProdCommon.MCPayloads.JobSpec import JobSpec

from WMComponent.JobEmulator.JobEmulator import JobEmulator
from WMCore.Agent.Configuration import Configuration
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class JobEmulatorTest(unittest.TestCase):

    _setup_done = False
    _teardown = True

    def setUp(self):

        if not JobEmulatorTest._setup_done:
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            # notice we include other parts of the schema here too.
            self.testInit.setSchema(['WMCore.WMBS', 'WMComponent.JobEmulator.Database'])
            JobEmulatorTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if JobEmulatorTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            self.testInit.clearDatabase()
        JobEmulatorTest._teardown = False


    def testA(self):
        # before we start we create a message service instance 
        # to receive control messages.
        # tested it with 5000 jobs
        simulateJobs = 50

        myThread = threading.currentThread()
        factory = WMFactory("msgService", "WMCore.MsgService."+ \
            myThread.dialect)

        msgService = myThread.factory['msgService'].loadObject("MsgService")
        myThread.transaction.begin()
        msgService.registerAs("JobEmulator_t")
        msgService.subscribeTo("JobEmulator:ControlMsg_JobFinished")
        myThread.transaction.commit()

         
        config = self.testInit.getConfiguration(os.path.join(os.getenv('WMCOREBASE'),'src/python/WMComponent/JobEmulator/DefaultConfig.py'))
        jobEmulator = JobEmulator(config)
        jobEmulator.prepareToStart()
        # make sure database settings are set properly.
        # the payload is a tupple: sites,nodes per site
        jobEmulator.handleMessage('JobEmulator:Reset','10,100')
        # generate some jobspecs.
        for i in xrange(0, simulateJobs):
            jobspec = JobSpec()
            jobspec.parameters['JobName'] = 'job_'+str(i)
            jobspecFileLocation = os.path.join(os.getenv('TESTDIR'), 'jobspec'+str(i)+'.xml')
            jobspec.save(jobspecFileLocation)
            jobEmulator.handleMessage('JobEmulator:JobSubmit',jobspecFileLocation) 
            # we know 2 messages have been sent (EmulateJob, and track) so invoke an empty handle message
            # as this will use the message queu.
            jobEmulator.handleMessage()
            jobEmulator.handleMessage()

        # we know that the same number of jobs will finish as started
        # so wait for the control messages. There are 2 per job
        for i in xrange(0, simulateJobs):
            msg = msgService.get() 
            msgService.finish()
        print('Received all control messages for job finsished')
        # we deliberately separate subscription as the track messages are numerous.
        myThread.transaction.begin()
        msgService.subscribeTo("JobEmulator:ControlMsg_JobTracked")
        myThread.transaction.commit()
        for i in xrange(0, simulateJobs):
            msg = msgService.get() 
            msgService.finish()
        print('Received all control messages for job tracking')

        # Terminate threads otherwise we do not exit.
        myThread = threading.currentThread()
        myThread.workerThreadManager.terminateWorkers()
    def runTest(self):

        #Run test.
        self.testA()

if __name__ == '__main__':
    unittest.main()
