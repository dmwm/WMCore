#!/bin/env python



import unittest
import threading
import os
import os.path


from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job
from WMComponent.JobSubmitter.JobSubmitter import JobSubmitter
from WMCore.JobStateMachine.ChangeState import ChangeState
from subprocess import Popen, PIPE

class JobSubmitterTest(unittest.TestCase):
    """
    Test class for the JobSubmitter

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']

    def setUp(self):
        """
        Standard setup


        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.MsgService'])
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.MsgService'],
                                useDefault = False)
        
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationSlots  = daofactory(classname = "Locations.SetJobSlots")

        for site in self.sites:
            locationAction.execute(siteName = site)
            locationSlots.execute(siteName = site, jobSlots = 1000)
        return

    def tearDown(self):
        """
        Standard tearDown

        """
        self.testInit.clearDatabase()



    def createJobGroup(self, nSubs, config, instance = '', workloadSpec = None, nJobs = 100):
        """
        This function acts to create a number of test job group with jobs that we can submit

        """

        myThread = threading.currentThread()

        jobGroupList = []

        changeState = ChangeState(config)

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = "wf001", task="basicWorkloadWithSandbox/Processing")
        testWorkflow.create()

        for i in range(0, nSubs):

            nameStr = str(instance) + str(i)

            if not workloadSpec:
                workloadSpec = "TestSingleWorkload%s/TestHugeTask" %(nameStr)

            myThread.transaction.begin()


        
            testFileset = Fileset(name = "TestFileset"+nameStr)
            testFileset.create()
        

            for j in range(0,nJobs):
                #pick a random site
                site = self.sites[0]
                testFile = File(lfn = "/singleLfn"+nameStr+str(j), size = 1024, events = 10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)


            testFileset.commit()
            testSubscription = Subscription(fileset = testFileset, workflow = testWorkflow, type = "Processing", split_algo = "FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            for file in testFileset.getFiles():
                testJob = Job(name = "test-%s" %(file["lfn"]))
                testJob.addFile(file)
                testJob["location"]  = file.getLocations()[0]
                testJob.create(testJobGroup)
                testJob.setCache(os.getcwd())
                testJobGroup.add(testJob)

            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

            myThread.transaction.commit()

            changeState.propagate(testJobGroup.jobs, 'created', 'new')

        return jobGroupList
        

    def getConfig(self, configPath = os.path.join(os.getenv('WMCOREBASE'), 'src/python/WMComponent/JobSubmitter/DefaultConfig.py')):
        """
        _getConfig_

        Gets a basic config from default location
        """
        config = self.testInit.getConfiguration(configPath)
        self.testInit.generateWorkDir( config )
        return config

    def testA_BasicSubmission(self):
        """
        This is the simplest of tests

        """

        #This submits a job to the local condor_q
        #It basically does nothing, so there's little to test
        
        myThread = threading.currentThread()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))
        if not os.path.isfile('basicWorkloadWithSandbox.pcl'):
            self.assertEqual(True, False, 'basicWorkloadWithSandbox.pcl must be present in working directory')

        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'first', workloadSpec = 'basicWorkloadWithSandbox')


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        print "You must check that you have 100 NEW jobs in the condor_q manually."
        print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        os.popen3('rm %s/*.jdl' %(config.JobSubmitter.submitDir))

        return


    def testB_TestLongSubmission(self):
        """
        See if you can get Burt to kill you.

        """

        #return

        myThread = threading.currentThread()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))
        if not os.path.isfile('basicWorkloadWithSandbox.pcl'):
            self.assertEqual(True, False, 'basicWorkloadWithSandbox.pcl must be present in working directory')

        #Right now only works with 1
        jobGroupList = self.createJobGroup(2, config, 'second', workloadSpec = 'basicWorkloadWithSandbox', nJobs = 1500)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        print "You must check that you have 3000 NEW jobs in the condor_q manually."
        print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        

        return



    def testC_ThisWillBreakEverything(self):
        """
        This test will fail.  I cannot make it work.

        """

        return

        myThread = threading.currentThread()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))
        if not os.path.isfile('basicWorkloadWithSandbox.pcl'):
            self.assertEqual(True, False, 'basicWorkloadWithSandbox.pcl must be present in working directory')

        #Right now only works with 1
        jobGroupList = self.createJobGroup(2, config, 'second', workloadSpec = 'basicWorkloadWithSandbox', nJobs = 5000)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)


        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)

        output = pipe.communicate()[0]

        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        print "You must check that you have 1000 NEW jobs in the condor_q manually."
        print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        

        return



if __name__ == "__main__":

    unittest.main() 
