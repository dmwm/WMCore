#!/usr/bin/env python

"""
JobTracker test 
"""




import os
import os.path
import logging
import threading
import unittest
import stat
import shutil
import subprocess
import getpass
import time
import cProfile
import pstats
from nose.plugins.attrib import attr

import WMCore.WMInit
from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job

from WMCore.DataStructs.Run   import Run

from WMComponent.JobTracker.JobTracker import JobTracker
from WMComponent.JobTracker.JobTrackerPoller import JobTrackerPoller

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.Agent.Configuration import Configuration


def createJDL(id, directory, jobCE):
    """
    _createJDL_

    Create a simple JDL string list
    """

    jdl = []


    jdl.append("universe = globus\n")
    jdl.append("should_transfer_executable = TRUE\n")
    jdl.append("notification = NEVER\n")
    jdl.append("Executable = %s/submit.sh\n" % (directory)) 
    jdl.append("Output = condor.$(Cluster).$(Process).out\n")
    jdl.append("Error = condor.$(Cluster).$(Process).err\n")
    jdl.append("Log = condor.$(Cluster).$(Process).log\n")
    jdl.append("initialdir = %s\n" % directory)
    jdl.append("globusscheduler = %s\n" % (jobCE))
    jdl.append("+WMAgent_JobID = %s\n" % id)
    jdl.append("+WMAgent_AgentName = testAgent\n")
    jdl.append("Queue 1")


    return jdl


def createSubmitScript(directory):
    """
    _createSubmitScript_

    Create a stupid submit script
    """


    script = """
    #!/bin/bash

    sleep 900

    exit 0
    """

    path = os.path.join(directory, 'submit.sh')

    f = open(path, 'w')
    f.write(script)
    f.close()

    os.chmod(path,
             stat.S_IXGRP | stat.S_IXOTH | stat.S_IXUSR | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IWUSR | stat.S_IWGRP)

    return
    

def getCondorRunningJobs(user):
    """
    _getCondorRunningJobs_

    Return the number of jobs currently running for a user
    """


    command = ['condor_q', user]
    pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE, shell = False)
    stdout, error = pipe.communicate()

    output = stdout.split('\n')[-2]

    nJobs = int(output.split(';')[0].split()[0])

    return nJobs



class JobTrackerTest(unittest.TestCase):
    """
    TestCase for TestJobTracker module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "malpaquet", seName = "malpaquet",
                               ceName = "malpaquet") 


        # We actually need the user name
        self.user = getpass.getuser()

        self.testDir = self.testInit.generateWorkDir()

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules = ["WMCore.WMBS", "WMCore.MsgService", "WMCore.ThreadPool"])

        self.testInit.delWorkDir()
        

    def getConfig(self):
        """
        _getConfig_

        Build a basic JobTracker config
        """
                       
        config = Configuration()

        config.section_("Agent")
        config.Agent.agentName  = 'testAgent'

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        # JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel      = 'INFO'
        config.JobTracker.pollInterval  = 10
        config.JobTracker.trackerName   = 'CondorTracker'
        config.JobTracker.pluginDir     = 'WMComponent.JobTracker.Plugins'
        config.JobTracker.componentDir  = os.path.join(os.getcwd(), 'Components')
        config.JobTracker.runTimeLimit  = 7776000 #Jobs expire after 90 days
        config.JobTracker.idleTimeLimit = 7776000
        config.JobTracker.heldTimeLimit = 7776000
        config.JobTracker.unknTimeLimit = 7776000


        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"
        
        return config
    



    def createTestJobs(self, nJobs, cacheDir):
        """
        _createTestJobs_

        Create several jobs
        """


        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow,
                                        type = "Processing",
                                        split_algo = "FileBased")
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        # Create a file
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()

        baseName = makeUUID()

        # Now create a job
        for i in range(nJobs):
            testJob = Job(name = '%s-%i' % (baseName, i))
            testJob.addFile(testFileA)
            testJob['location'] = 'malpaquet'
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJob.create(testJobGroup)
            testJobGroup.add(testJob)

        testJobGroup.commit()

        # Set test job caches
        for job in testJobGroup.jobs:
            job.setCache(cacheDir)

        return testJobGroup

        

    @attr('integration')
    def testA_CondorTest(self):
        """
        _CondorTest_
        
        Because I don't want this test to be submitter dependent:
        Create a dummy condor job.
        Submit a dummy condor job.
        Track it.
        Kill it.
        Exit
        """

        myThread = threading.currentThread()

        # This has to be run with an empty queue
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        nJobs  = 10
        jobCE  = 'cmsosgce.fnal.gov/jobmanager-condor'

        # Create directories
        cacheDir  = os.path.join(self.testDir, 'CacheDir')
        submitDir = os.path.join(self.testDir, 'SubmitDir')

        if not os.path.isdir(cacheDir):
            os.makedirs(cacheDir)
        if not os.path.isdir(submitDir):
            os.makedirs(submitDir)


        # Get config
        config = self.getConfig()

        # Get jobGroup
        testJobGroup = self.createTestJobs(nJobs = nJobs, cacheDir = cacheDir)

        # Propogate jobs
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')

        result = self.getJobs.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nJobs)

        jobTracker = JobTrackerPoller(config)
        jobTracker.setup()



        # First iteration
        # There are no jobs in the tracker,
        # The tracker should register the jobs as missing
        # This should tell it that they've finished
        # So the tracker should send them onwards
        jobTracker.algorithm()

        result = self.getJobs.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 0)


        result = self.getJobs.execute(state = 'complete', jobType = "Processing")
        self.assertEqual(len(result), 10)



        # Second iteration
        # Reset the jobs
        # This time submit them to the queue
        # The jobs should remain in holding
        changer.propagate(testJobGroup.jobs, 'executing', 'created')

        result = self.getJobs.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nJobs)

        # Create a submit script
        createSubmitScript(submitDir)

        # Now create some jobs
        for job in testJobGroup.jobs:
            jdl = createJDL(id = job['id'], directory = submitDir, jobCE = jobCE)
            jdlFile = os.path.join(submitDir, 'condorJDL_%i.jdl' % (job['id']))
            handle = open(jdlFile, 'w')
            handle.writelines(jdl)
            handle.close()

            command = ["condor_submit", jdlFile]
            pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE, shell = False)

        time.sleep(1)

        # All jobs should be running
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs)


        # Run the algorithm.  After this
        # all jobs should still be running
        jobTracker.algorithm()

        # Are jobs in the right state?
        result = self.getJobs.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nJobs)

        result = self.getJobs.execute(state = 'Complete', jobType = "Processing")
        self.assertEqual(len(result), 0)

        # Are jobs still in the condor_q
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs)


        # Then we're done
        killList = [x['id'] for x in testJobGroup.jobs]
        jobTracker.killJobs(jobList = killList)

        # No jobs should be left
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0)
            

        # This is optional if you want to look at what
        # files were actually created during running
        #if os.path.isdir('testDir'):
        #    shutil.rmtree('testDir')
        #shutil.copytree('%s' %self.testDir, os.path.join(os.getcwd(), 'testDir'))


        return

    @attr('integration')
    def testB_ReallyLongTest(self):
        """
        _ReallyLongTest_

        Run a really long test using the condor plugin
        """

        #return

        # This has to be run with an empty queue
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))



        myThread = threading.currentThread()

        # This has to be run with an empty queue
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        nJobs  = 500
        jobCE  = 'cmsosgce.fnal.gov/jobmanager-condor'

        # Create directories
        cacheDir  = os.path.join(self.testDir, 'CacheDir')
        submitDir = os.path.join(self.testDir, 'SubmitDir')

        if not os.path.isdir(cacheDir):
            os.makedirs(cacheDir)
        if not os.path.isdir(submitDir):
            os.makedirs(submitDir)


        # Get config
        config = self.getConfig()

        # Get jobGroup
        testJobGroup = self.createTestJobs(nJobs = nJobs, cacheDir = cacheDir)

        # Propogate jobs
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')



        jobTracker = JobTrackerPoller(config)
        jobTracker.setup()


        # Now create some jobs
        for job in testJobGroup.jobs[:(nJobs/2)]:
            jdl = createJDL(id = job['id'], directory = submitDir, jobCE = jobCE)
            jdlFile = os.path.join(submitDir, 'condorJDL_%i.jdl' % (job['id']))
            handle = open(jdlFile, 'w')
            handle.writelines(jdl)
            handle.close()

            command = ["condor_submit", jdlFile]
            pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE, shell = False)
            pipe.communicate()


        startTime = time.time()
        cProfile.runctx("jobTracker.algorithm()", globals(), locals(), filename = "testStats.stat")
        #jobTracker.algorithm()
        stopTime  = time.time()


        # Are jobs in the right state?
        result = self.getJobs.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nJobs/2)

        result = self.getJobs.execute(state = 'Complete', jobType = "Processing")
        self.assertEqual(len(result), nJobs/2)


        # Then we're done
        killList = [x['id'] for x in testJobGroup.jobs]
        jobTracker.killJobs(jobList = killList)

        # No jobs should be left
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0)


        print ("Process took %f seconds to process %i classAds" %((stopTime - startTime),
                                                                  nJobs/2))


        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()

        return

        


if __name__ == '__main__':
    unittest.main()

