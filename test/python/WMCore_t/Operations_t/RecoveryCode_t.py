#!/usr/bin/env python

import unittest
import os
import os.path
import logging
import threading
import shutil

from WMCore.Agent.Configuration import Configuration
from WMQuality.TestInit         import TestInit

from WMCore.Operations.RecoveryCode import PurgeJobs

from subprocess import Popen, PIPE
import WMCore.WMInit

class TestRecoveryCode(unittest.TestCase):
    """
    Test for recoveryCode; disaster recovery system
    """

    #This is an integration test
    __integration__ = "So this guy walks into a bar..."


    def setUp(self):
        """
        Mimic remains of destroyed job

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.nJobs = 10

        os.mkdir('test')
        os.mkdir('test/Basic')
        os.mkdir('test/Basic/Crap')
        os.mkdir('test/Basic/Crap/JobCollection_1')
        for i in range(self.nJobs):
            os.mkdir('test/Basic/Crap/JobCollection_1/Job_%i' % (i))

        self.logLevel = 'INFO'

        #There must be a submit directory for jobSubmitter
        if not os.path.isdir('submit'):
            os.mkdir('submit')

        #There must be a log directory for jobArchiver
        if not os.path.isdir('logs'):
            os.mkdir('logs')




    def tearDown(self):
        """
        Check to make sure that everything is good.

        """

        if os.path.isdir('test'):
            shutil.rmtree('test')
        for file in os.listdir('logs'):
            #You have to remove the staged out logs
            os.remove(os.path.join('logs', file))




    def getConfig(self):
        """
        _getConfig_
        
        For now, build a config file from the ground up.
        Later, use this as a model for the JSM master config
        """

        myThread = threading.currentThread()

        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.dialect    = os.getenv("DIALECT")
        myThread.dialect               = os.getenv('DIALECT')
        config.CoreDatabase.user       = os.getenv("DBUSER", os.getenv("USER"))
        config.CoreDatabase.hostname   = os.getenv("DBHOST", os.getenv("HOSTNAME"))
        config.CoreDatabase.passwd     = os.getenv("DBPASS")
        config.CoreDatabase.name       = os.getenv("DBNAME", os.getenv("DATABASE"))
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.dbsock     = os.getenv("DBSOCK")

        if not config.CoreDatabase.dialect or not config.CoreDatabase.connectUrl:
            msg1 = "No database or dialect in environment!"
            msg2 = "Database set to %s" %(config.CoreDatabase.connectUrl)
            msg3 = "Dialect set to %s" %(config.CoreDatabase.dialect)
            print msg1
            print msg2
            print msg3
            raise Exception (msg1)


        #General config options
        config.section_("WMAgent")
        config.WMAgent.WMSpecDirectory    = os.getcwd()  #Where are the WMSpecs by default?


        #Now we go by component

        #First the JobCreator
        config.component_("JobCreator")
        config.JobCreator.namespace        = 'WMComponent.JobCreator.JobCreator'
        config.JobCreator.logLevel         = self.logLevel
        config.JobCreator.maxThreads       = 1
        config.JobCreator.UpdateFromSiteDB = True
        config.JobCreator.pollInterval     = 10
        config.JobCreator.jobCacheDir      = os.path.join(os.getcwd(), 'test')
        config.JobCreator.defaultJobType   = 'processing' #Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads    = 2
        config.JobCreator.componentDir     = os.path.join(os.getcwd(), 'Components/JobCreator')

        #JobMaker
        config.component_('JobMaker')
        config.JobMaker.logLevel        = self.logLevel
        config.JobMaker.namespace       = 'WMCore.WMSpec.Makers.JobMaker'
        config.JobMaker.maxThreads      = 1
        config.JobMaker.makeJobsHandler = 'WMCore.WMSpec.Makers.Handlers.MakeJobs'


        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'mnorman:theworst@cmssrv48.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        #JobSubmitter
        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel      = self.logLevel
        config.JobSubmitter.maxThreads    = 1
        config.JobSubmitter.pollInterval  = 10
        config.JobSubmitter.pluginName    = 'ShadowPoolPlugin'
        config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir     = os.path.join(os.getcwd(), 'submit')
        config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        config.JobSubmitter.submitScript  = os.path.join(os.getcwd(), 'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components/JobSubmitter')
        config.JobSubmitter.inputFile     = os.path.join(os.getcwd(), 'FrameworkJobReport-4540.xml')
        config.JobSubmitter.workerThreads = 1
        config.JobSubmitter.jobsPerWorker = 100


        #JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel      = self.logLevel
        config.JobTracker.pollInterval  = 10
        config.JobTracker.trackerName   = 'TestTracker'
        config.JobTracker.pluginDir     = 'WMComponent.JobTracker.Plugins'
        config.JobTracker.runTimeLimit  = 7776000 #Jobs expire after 90 days
        config.JobTracker.idleTimeLimit = 7776000
        config.JobTracker.heldTimeLimit = 7776000
        config.JobTracker.unknTimeLimit = 7776000
        

        #ErrorHandler
        config.component_("ErrorHandler")
        config.ErrorHandler.logLevel     = self.logLevel
        config.ErrorHandler.namespace    = 'WMComponent.ErrorHandler.ErrorHandler'
        config.ErrorHandler.maxThreads   = 30
        config.ErrorHandler.maxRetries   = 10
        config.ErrorHandler.pollInterval = 10
        

        #RetryManager
        config.component_("RetryManager")
        config.RetryManager.logLevel     = self.logLevel
        config.RetryManager.namespace    = 'WMComponent.RetryManager.RetryManager'
        config.RetryManager.maxRetries   = 10
        config.RetryManager.pollInterval = 10
        config.RetryManager.coolOffTime  = {'create': 10, 'submit': 10, 'job': 10}
        config.RetryManager.pluginPath   = 'WMComponent.RetryManager.PlugIns'
        config.RetryManager.pluginName   = ''
        config.RetryManager.WMCoreBase   = WMCore.WMInit.getWMBASE()
        

        #JobAccountant
        config.component_("JobAccountant")
        config.JobAccountant.logLevel      = self.logLevel
        #config.JobAccountant.logLevel      = 'SQLDEBUG'
        config.JobAccountant.pollInterval  = 10
        config.JobAccountant.workerThreads = 1
        config.JobAccountant.componentDir  = os.path.join(os.getcwd(), 'Components/JobAccountant')


        #JobArchiver
        config.component_("JobArchiver")
        config.JobArchiver.pollInterval  = 10
        config.JobArchiver.logLevel      = self.logLevel
        #config.JobArchiver.logLevel      = 'SQLDEBUG'
        config.JobArchiver.logDir        = os.path.join(os.getcwd(), 'logs')

        #DBSBuffer
        #Part of the JobAccountant
        config.component_("DBSBuffer")
        config.DBSBuffer.logLevel          = self.logLevel
        config.DBSBuffer.namespace         = 'WMComponent.DBSBuffer.DBSBuffer'
        config.DBSBuffer.maxThreads        = 1
        config.DBSBuffer.jobSuccessHandler = 'WMComponent.DBSBuffer.Handler.JobSuccess'



        return config


    def submitJobs(self, nJobs):
        """
        _submitJobs_
        
        Submit some broken jdls to the local condor submitter
        """


        submitFile = """
universe = globus
globusscheduler = thisisadummyname.fnal.gov/jobmanager-suck
should_transfer_executable = TRUE
transfer_output_files = FrameworkJobReport.xml
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
log_xml = True
notification = NEVER
Output = condor.$(Cluster).$(Process).out
Error = condor.$(Cluster).$(Process).err
Log = condor.$(Cluster).$(Process).log
Executable = /home/mnorman/WMCORE/test/python/WMCore_t/Operations_t/submit.sh
initialdir = /home/mnorman/WMCORE/test/python/WMCore_t/Operations_t/test/Basic/Crap/JobCollection_1/Job_%i
+WMAgent_JobName = \"65bf3894-d873-11de-9e40-0030482c2dd0-1\"
+WMAgent_JobID = %i
Queue 1

        """

        for i in range(10):
            f = open('submit/submit_%i.jdl' %(i), 'w')
            f.write(submitFile % (i, i))
            f.close()
            command = ["condor_submit", 'submit/submit_%i.jdl' %(i)]
            pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
            pipe.wait()
            


    def testPurge(self):
        """
        _testPurge_
        
        Test the purge function, which should remove all job objects
        """

        config = self.getConfig()

        self.submitJobs(self.nJobs)

        self.assertEqual(len(os.listdir('submit')), self.nJobs, 'Only found %i submit files' %(len(os.listdir('submit'))))
        self.assertEqual(len(os.listdir('logs')), 0, 'Please empty the logs directory')

        #Check that ten jobs were actually submitted
        jobCheckString = os.popen('condor_q %s' %os.getenv('USER')).readlines()[-1]
        self.assertEqual(jobCheckString, '%i jobs; %i idle, 0 running, 0 held\n' % (self.nJobs, self.nJobs))

        purgeJobs = PurgeJobs(config)

        purgeJobs.run()

        self.assertEqual(os.listdir('test'), [])
        self.assertEqual(len(os.listdir('logs')), self.nJobs, \
                         'Found %i tarballs instead of %i in logOut directory' \
                         %(len(os.listdir('logs')), self.nJobs) )
        self.assertEqual(os.listdir('submit'), [])


        #Check that jobs were actually removed
        jobCheckString = os.popen('condor_q %s' %os.getenv('USER')).readlines()[-1]
        self.assertEqual(jobCheckString, '0 jobs; 0 idle, 0 running, 0 held\n' )
        
        return


if __name__ == "__main__":

    unittest.main() 
