#!/bin/env python


__revision__ = "$Id: JobSubmitter_t.py,v 1.15 2010/06/08 20:08:19 mnorman Exp $"
__version__ = "$Revision: 1.15 $"

import unittest
import threading
import os
import os.path
import time
import shutil
import pickle
import cProfile
import pstats
import copy
import getpass
import re
import cPickle

from subprocess import Popen, PIPE


import WMCore.WMInit
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit import getWMBASE

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job



from WMComponent.JobSubmitter.JobSubmitter       import JobSubmitter
from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMComponent.JobSubmitter.Plugins.CondorGlobusPlugin   import CondorGlobusPlugin
from WMComponent.JobSubmitter.Plugins.BossLiteCondorPlugin import BossLiteCondorPlugin


from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.Services.UUID import makeUUID

from WMCore.Agent.Configuration             import loadConfigurationFile, Configuration
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.DataStructs.JobPackage          import JobPackage


#Workload stuff
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMSpec.StdSpecs.ReReco  import rerecoWorkload, getTestArguments


def parseJDL(jdlLocation):
    """
    _parseJDL_

    Parse a JDL into some sort of meaningful dictionary
    """


    f = open(jdlLocation, 'r')
    lines = f.readlines()
    f.close()


    listOfJobs = []
    headerDict = {}
    jobLines   = []

    index = 0

    for line in lines:
        # Go through the lines until you hit Queue
        index += 1
        splits = line.split(' = ')
        key = splits[0]
        value = ' = '.join(splits[1:])
        value = value.rstrip('\n')
        headerDict[key] = value
        if key == "Queue 1\n":
            # Yes, this is clumsy
            jobLines = lines[index:]
            break

    tmpDict = {}
    index   = 2
    for jobLine in jobLines:
        splits = jobLine.split(' = ')
        key = splits[0]
        value = ' = '.join(splits[1:])
        value = value.rstrip('\n')
        if key == "Queue 1\n":
            # Then we've hit the next block
            tmpDict["index"] = index
            listOfJobs.append(tmpDict)
            tmpDict = {}
            index += 1
        else:
            tmpDict[key] = value

    if tmpDict != {}:
        listOfJobs.append(tmpDict)


    return listOfJobs, headerDict



def getCondorRunningJobs(user):
    """
    _getCondorRunningJobs_

    Return the number of jobs currently running for a user
    """


    command = ['condor_q', user]
    pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
    stdout, error = pipe.communicate()

    output = stdout.split('\n')[-2]

    #output = pipe.stdout.readlines()

    print output

    nJobs = int(output.split(';')[0].split()[0])

    return nJobs


        

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
        #self.testInit.clearDatabase(modules = ['WMCore.WMBS', 'WMCore.MsgService', 'WMCore.ResourceControl'])
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.MsgService', 'WMCore.ResourceControl', 'WMCore.BossLite'],
                                useDefault = False)
        
        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        
        locationAction = self.daoFactory(classname = "Locations.New")
        locationSlots  = self.daoFactory(classname = "Locations.SetJobSlots")

        # We actually need the user name
        self.user = getpass.getuser()

        #self.ceName = 'cms-sleepgw.fnal.gov/jobmanager-condor'
        #self.ceName = 'cmsosgce.fnal.gov/jobmanager-condor'
        #self.ceName = 'thisisnotarealserver.fnal.gov'
        self.ceName = '127.0.0.1'

        for site in self.sites:
            locationAction.execute(siteName = site, seName = site, ceName = self.ceName)
            locationSlots.execute(siteName = site, jobSlots = 1000)


        #Create sites in resourceControl
        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = site, ceName = self.ceName)
            resourceControl.insertThreshold(siteName = site, taskType = 'Processing', \
                                            minSlots = 1000, maxSlots = 10000)


        self.testDir = self.testInit.generateWorkDir()
            
        return

    def tearDown(self):
        """
        Standard tearDown

        """
        #self.testInit.clearDatabase(modules = ['WMCore.ResourceControl', 'WMCore.WMBS', 'WMCore.MsgService'])
        self.testInit.clearDatabase()

        self.testInit.delWorkDir()



    def createJobGroups(self, nSubs, nJobs, task, workloadSpec):
        """
        Creates a series of jobGroups for submissions

        """

        jobGroupList = []

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = "wf001", task="basicWorkload/Production")
        testWorkflow.create()

        # Create subscriptions
        for i in range(nSubs):

            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name = name)
            testFileset.create()
            testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()


            # Create jobs
            self.makeNJobs(name = name, task = task,
                           nJobs = nJobs,
                           jobGroup = testJobGroup,
                           fileset = testFileset,
                           sub = i)
                                         


            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        return jobGroupList
            


    def makeNJobs(self, name, task, nJobs, jobGroup, fileset, sub):
        """
        _makeNJobs_

        Make and return a WMBS Job and File
        This handles all those damn add-ons

        """
        # Set the CacheDir
        cacheDir = os.path.join(self.testDir, 'CacheDir')


        for n in range(nJobs):
            # First make a file
            site = self.sites[0]
            testFile = File(lfn = "/singleLfn/%s/%s" %(name, n),
                            size = 1024, events = 10)
            testFile.setLocation(site)
            testFile.create()
            fileset.addFile(testFile)

        fileset.commit()

        index = 0
        for f in fileset.files:
            index += 1
            testJob = Job(name = '%s-%i' %(name, index))
            testJob.addFile(f)
            testJob["location"]  = f.getLocations()[0]
            testJob['task']    = task.getPathName()
            testJob['sandbox'] = task.data.input.sandbox
            testJob['spec']    = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            output = open(os.path.join(jobCache, 'job.pkl'),'w')
            pickle.dump(testJob, output)
            output.close()

        return testJob, testFile
        

    def getConfig(self, configPath = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMComponent/JobSubmitter/DefaultConfig.py')):
        """
        _getConfig_

        Gets a basic config from default location
        """

        myThread = threading.currentThread()

        config = Configuration()

        config.component_("Agent")
        config.Agent.WMSpecDirectory = self.testDir
        config.Agent.agentName       = 'testAgent'


        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel      = 'INFO'
        config.JobSubmitter.maxThreads    = 1
        config.JobSubmitter.pollInterval  = 10
        config.JobSubmitter.pluginName    = 'CondorGlobusPlugin'
        config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir     = os.path.join(self.testDir, 'submit')
        config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        config.JobSubmitter.submitScript  = os.path.join(WMCore.WMInit.getWMBASE(),
                                                         'test/python/WMComponent_t/JobSubmitter_t',
                                                         'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200
        config.JobSubmitter.inputFile     = os.path.join(WMCore.WMInit.getWMBASE(),
                                                         'test/python/WMComponent_t/JobSubmitter_t',
                                                         'FrameworkJobReport-4540.xml')


        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL',
                                                           'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        # Needed, because this is a test
        os.makedirs(config.JobSubmitter.submitDir)


        return config
    
    def createTestWorkload(self, workloadName = 'Test', emulator = True):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """
        # Create a new workload using StdSpecs.ReReco
        #arguments = {
        #    "CmsPath": "/uscmst1/prod/sw/cms",
        #    "AcquisitionEra": "WMAgentCommissioning10",
        #    "Requester": "sfoulkes@fnal.gov",
        #    "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        #    "CMSSWVersion": "CMSSW_3_5_8_patch3",
        #    "ScramArch": "slc5_ia32_gcc434",
        #    "ProcessingVersion": "v2scf",
        #    "SkimInput": "output",
        #    "GlobalTag": "GR10_P_v4::All",
        #    
        #    "ProcessingConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8",
        #    "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1",
        #    
        #    "CouchUrl": "http://dmwmwriter:gutslap!@cmssrv52.fnal.gov:5984",
        #    "CouchDBName": "wmagent_config_cache",
        #    "Scenario": "",
        #    "Emulate" : emulator,
        #    }


        arguments = getTestArguments()

        workload = rerecoWorkload("Tier1ReReco", arguments)
        rereco = workload.getTask("ReReco")

        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload


    def checkJDL(self, config, cacheDir, submitFile):
        """
        _checkJDL_

        Check the basic JDL setup
        """

        jobs, head = parseJDL(jdlLocation = os.path.join(config.JobSubmitter.submitDir,
                                                         submitFile))


        # Check each job entry in the JDL
        for job in jobs:
            # Check each key
            index = job.get('index', 0)
            self.assertTrue(index != 1)
            self.assertTrue('+WMAgent_JobName' in job.keys())
            # TODO: Think of a better way to do this
            #self.assertEqual(job.get('initialdir', None),
            #                 os.path.join(cacheDir, 'Job_%i' % index))
            self.assertEqual(job.get('+WMAgent_JobID', 0), str(index))
            self.assertEqual(job.get('globusscheduler', None), self.ceName)
            inputFileString = '%s, %s, %s' % (os.path.join(self.testDir, 'workloadTest/Tier1ReReco', 'Tier1ReReco-Sandbox.tar.bz2'),
                                              os.path.join(self.testDir, 'workloadTest/Tier1ReReco', 'batch_1/JobPackage.pkl'),
                                              os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMCore', 'WMRuntime/Unpacker.py'))
            self.assertEqual(job.get('transfer_input_files', None),
                             inputFileString)
            # Arguments use a list starting from 0
            self.assertEqual(job.get('arguments', None),
                             'Tier1ReReco-Sandbox.tar.bz2 %i' % (index - 1))

        # Now handle the head
        self.assertEqual(head.get('should_transfer_files', None), 'YES')
        self.assertEqual(head.get('Log', None), 'condor.$(Cluster).$(Process).log')
        self.assertEqual(head.get('Error', None), 'condor.$(Cluster).$(Process).err')
        self.assertEqual(head.get('Output', None), 'condor.$(Cluster).$(Process).out')
        self.assertEqual(head.get('transfer_output_remaps', None),
                         '\"Report.pkl = Report.$(Cluster).$(Process).pkl\"')
        self.assertEqual(head.get('when_to_transfer_output', None), 'ON_EXIT')
        self.assertEqual(head.get('Executable', None), config.JobSubmitter.submitScript)



    def testA_BasicTest(self):
        """
        Use the CondorGlobusPlugin to create a very simple test
        Check to see that all the jobs were submitted
        Parse and test the JDL files
        See what condor says
        """

        workloadName = "basicWorkload"

        myThread = threading.currentThread()

        workload = self.createTestWorkload()

        config   = self.getConfig()

        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 10
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')


        # Do pre-submit check
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))
        

        jobSubmitter = JobSubmitterPoller(config = config)
        jobSubmitter.algorithm()


        # Check that jobs are in the right state
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        
        # Check on the JDL
        submitFile = os.listdir(config.JobSubmitter.submitDir)[0]
        self.checkJDL(config = config, cacheDir = cacheDir, submitFile = submitFile)


        # Check to make sure we have running jobs
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs * nSubs)


        # Now clean-up
        command = ['condor_rm', self.user]
        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.communicate()        

        
        return



    def testB_TimeLongSubmission(self):
        """
        _TimeLongSubmission_

        Submit a lot of jobs and test how long it takes for
        them to actually be submitted
        """


        #return


        workloadName = "basicWorkload"
        myThread     = threading.currentThread()
        workload     = self.createTestWorkload()
        config       = self.getConfig()
        changeState  = ChangeState(config)

        nSubs = 5
        nJobs = 300
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))


        jobSubmitter = JobSubmitterPoller(config = config)

        # Actually run it
        startTime = time.time()
        cProfile.runctx("jobSubmitter.algorithm()", globals(), locals(), filename = "testStats.stat")
        #jobSubmitter.algorithm()
        stopTime  = time.time()

        if os.path.isdir('CacheDir'):
            shutil.rmtree('CacheDir')
        shutil.copytree('%s' %self.testDir, os.path.join(os.getcwd(), 'CacheDir'))


        # Check to make sure we have running jobs
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs * nSubs)


        # Now clean-up
        command = ['condor_rm', self.user]
        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.communicate()


        print "Job took %f seconds to complete" %(stopTime - startTime)


        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()

        return



    def testC_TestPlugin(self):
        """
        Run the plugin directly.

        This one is a bit weird...
        """


        #return

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        workloadName = "basicWorkload"

        myThread = threading.currentThread()

        workload = self.createTestWorkload()

        config   = self.getConfig()
        config.JobSubmitter.pluginName    = 'BossLiteCondorPlugin'

        changeState = ChangeState(config)

        nSubs = 2
        nJobs = 250
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')


        # Do pre-submit check
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)


        # Now create subscription bundles
        subBundle = []
        for jobGroup in jobGroupList:
            jobList = []

            sandbox = jobGroup.jobs[0]['sandbox']
            
            # Create job package
            package = JobPackage()
            for job in jobGroup.jobs:
                tmpJob = Job(id = job['id'])
                tmpJob['custom']      = {'location': 'T2_US_UCSD'}
                tmpJob['name']        = job['name']
                tmpJob['cache_dir']   = job['cache_dir']
                tmpJob['retry_count'] = job['retry_count']
                jobList.append(tmpJob)
                package.append(job.getDataStructsJob())
            package.save(os.path.join(self.testDir, 'JobPackage.pkl'))

                
            subDict = {}
            subDict['packageDir'] = self.testDir
            subDict['index']      = 0
            subDict['sandbox']    = sandbox
            subDict['jobs']       = jobList
            subBundle.append(subDict)


        # Now we submit them
        #plugin = BossLiteCondorPlugin(submitDir    = config.JobSubmitter.submitDir,
        plugin = CondorGlobusPlugin(submitDir    = config.JobSubmitter.submitDir,
                                    submitScript = config.JobSubmitter.submitScript)

        startTime = time.time()
        cProfile.runctx("plugin(parameters = subBundle)", globals(), locals(), filename = "profStats.stat")
        #plugin(parameters = subBundle)
        stopTime  = time.time()


        # Check to make sure we have running jobs
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs * nSubs)



        # Now clean-up
        command = ['condor_rm', self.user]
        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.communicate()


        print "Job took %f seconds to run plugin" % (stopTime - startTime)

        p = pstats.Stats('profStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()

        return

        


    def testB_TestLongSubmission(self):
        """
        See if you can get Burt to kill you.

        """

        return

        myThread = threading.currentThread()

        workload = self.createTestWorkload()

        config = self.getConfig()
        #config.JobSubmitter.submitDir = config.General.workDir
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))


        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'second', workloadSpec = 'basicWorkload', task = workload.getTask('Production'), nJobs = 500)


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

        #print "You must check that you have 3000 NEW jobs in the condor_q manually."
        #print "WARNING!  REMOVE YOUR JOB FROM THE CONDOR_Q!"

        

        return




    def testD_shadowPoolSubmit(self):


        return

        workload = self.createTestWorkload()

        myThread = threading.currentThread()

        #if os.path.exists(os.path.join(os.getcwd(), 'FrameworkJobReport.xml')):
        #    os.remove(os.path.join(os.getcwd(), 'FrameworkJobReport.xml'))

        config = self.getConfig()
        config.JobSubmitter.pluginName    = 'CondorGlobusPlugin'
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))

        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'second', workloadSpec = 'basicWorkload', task = workload.getTask('Production'), nJobs = 10)


        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        #Give it three minutes to get on a node and do its 120 second of sleeping
        time.sleep(90)


        print myThread.dbi.processData("SELECT * FROM wmbs_location")[0].fetchall()
        
        username = os.getenv('USER')
        pipe = Popen(['condor_q', username], stdout = PIPE, stderr = PIPE, shell = True)
        output = pipe.communicate()[0]
        self.assertEqual(output.find(username) > 0, True, "I couldn't find your username in the local condor_q.  Check it manually to find your job")

        time.sleep(180)

        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        if os.path.isdir('CacheDir'):
            shutil.rmtree('CacheDir')
        shutil.copytree('%s' %self.testDir, os.path.join(os.getcwd(), 'CacheDir'))

        result = myThread.dbi.processData("SELECT state FROM wmbs_job")[0].fetchall()

        for state in result:
            self.assertEqual(state.values()[0], 14)

        self.assertEqual(os.path.isfile('%s/CacheDir/Job_2/Report.pkl' %self.testDir), True, "Job did not return file successfully")
        self.assertEqual(os.path.isfile('%s/CacheDir/Job_8/Report.pkl' %self.testDir), True, "Job did not return file successfully")

        fileStats = os.stat('%s/CacheDir/Job_8/Report.pkl' %self.testDir)
        self.assertEqual(int(fileStats.st_size) > 0, True, "Job returned zero-length file")

        
        

        return



    def testE_shadowPoolLongTest(self):
        """
        If you run this, Burt will hunt you down and kill you

        """

        return

        workload = self.createTestWorkload()

        myThread = threading.currentThread()

        config = self.getConfig()
        config.JobSubmitter.pluginName    = 'CondorGlobusPlugin'
        if not os.path.isdir(config.JobSubmitter.submitDir):
            self.assertEqual(True, False, "This code cannot run without a valid submit directory %s (from config)" %(config.JobSubmitter.submitDir))

        #Right now only works with 1
        jobGroupList = self.createJobGroup(1, config, 'second', workloadSpec = 'basicWorkload', task = workload.getTask('Production'), nJobs = 950)


        startTime = time.clock()

        # some general settings that would come from the general default 
        # config file

        testJobSubmitter = JobSubmitter(config)
        testJobSubmitter.prepareToStart()

        #Should run twice and end
        myThread.workerThreadManager.terminateWorkers()


        stopTime = time.clock()

        print "Job took %f seconds" %(stopTime - startTime)
        
        return




if __name__ == "__main__":

    unittest.main() 
