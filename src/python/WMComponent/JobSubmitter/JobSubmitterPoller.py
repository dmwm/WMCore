#!/usr/bin/env python

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.1 2009/10/07 19:33:35 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"


#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import time
import os.path
import string
#import common

#WMBS objects
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job as Job
from WMCore.WMBS.Workflow     import Workflow
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory

from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                     import WMTask, WMTaskHelper


from WMCore.JobStateMachine.ChangeState import ChangeState


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread




class JobSubmitterPoller(BaseWorkerThread):
    """
    Handles job submission

    """

    def __init__(self, config):

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        #Dictionary definitions
        self.slots = {}
        self.sites = {}

        #Set config objects
        self.database = config.CoreDatabase.connectUrl
        self.dialect  = config.CoreDatabase.dialect

        self.session = None
        self.schedulerConfig = {}
        self.cfg_params = None
        self.config = config

        BaseWorkerThread.__init__(self)

        return

    def algorithm(self, parameters):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobSubmitter")
        myThread = threading.currentThread()
        try:
            startTime = time.clock()
            self.runSubmitter()
            stopTime = time.clock()
            logging.debug("Running jobSubmitter took %f seconds" %(stopTime - startTime))
            #print "Runtime for JobSubmitter %f" %(stopTime - startTime)
            #print self.timing
        except:
            myThread.transaction.rollback()
            raise

    def runSubmitter(self):
        """
        _runSubmitter_

        Keeps track of, and does, everything
        """

        myThread = threading.currentThread()

        #self.configure()
        self.setLocations()
        self.pollJobs()
        jobList = self.getJobs()
        jobList = self.setJobLocations(jobList)
        jobList = self.grabTask(jobList)
        self.submitJobs(jobList)

        return


#    def configure(self, cfg_params = {}):
#        
#        myThread = threading.currentThread()
#
#
#        self.cfg_params = cfg_params
#        if not self.cfg_params.has_key('scheduler'):
#            self.cfg_params['scheduler'] = 'fake'
#
#        
#        #self.deep_debug= self.cfg_params.get("USER.deep_debug",'0')
#        #server_check =  self.cfg_params.get("CRAB.server_name",None)
#        #if self.deep_debug == '1' and server_check != None :
#        #    msg =  'You are asking the deep_debug, but it cannot works using the server.\n'
#        #    msg += '\t The functionality will not have effect.'
#        #    logging.info(msg) 
#        #self.schedulerName =  self.cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements
#        #self.rb_param_file=''
#        if not self.cfg_params.has_key('GRID.rb'):
#            self.cfg_params['GRID.rb']='CERN'
#        #self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("GRID.rb"))
#        #self.wms_service=cfg_params.get("GRID.wms_service",'')
#
#        #self.wrapper = cfg_params.get('CRAB.jobtype').upper()+'.sh'
#
#
#        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
#        SchedMap = {'glite':    'SchedulerGLiteAPI',
#                    'glitecoll':'SchedulerGLiteAPI',\
#                    'condor':   'SchedulerCondor',\
#                    'condor_g': 'SchedulerCondorG',\
#                    'glidein':  'SchedulerGlidein',\
#                    'lsf':      'SchedulerLsf',\
#                    'caf':      'SchedulerLsf',\
#                    'sge':      'SchedulerSge',
#                    'arc':      'SchedulerARC',
#                    'fake':     'SchedulerFake'
#                    }
#
#        #self.schedulerConfig = common.scheduler.realSchedParams(cfg_params)
#        self.schedulerConfig['name'] =  SchedMap[(self.cfg_params['scheduler']).lower()]
#        self.schedulerConfig['timeout'] = 180
#        self.schedulerConfig['skipProxyCheck'] = True 
#        self.schedulerConfig['logger'] = myThread.logger
#
#        self.session = None
#        return



    def setLocations(self):
        self.locations = {}

        #Then get all locations
        locationList            = self.daoFactory(classname = "Locations.List")
        locationSlots           = self.daoFactory(classname = "Locations.GetJobSlots")

        locations = locationList.execute()

        for loc in locations:
            location = loc[1]  #We need this because locations are returned as a list
            value = locationSlots.execute(siteName = location)
            self.slots[location] = value

        return

    def getJobs(self):
        """
        _getJobs_

        This uses WMBS to extract a list of jobs in the 'Created' state
        """

        getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList = getJobs.execute(state = "Created")

        return jobList

    def setJobLocations(self, jobList, whiteList = [], blackList = []):
        """
        _setJobLocations

        Set the locations for each job based on current knowledge
        """

        newList = []

        for jid in jobList:
            job = Job(id = jid)
            location = self.findSiteForJob(job)
            job["location"] = location
            newList.append(job)

        return newList

    def findSiteForJob(self, job):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        myThread = threading.currentThread()

        #Assume that jobSplitting has worked, and that every file has the same set of locations
        sites = list(job.getFiles()[0]['locations'])

        tmpSite  = ''
        tmpSlots = 0
        for loc in sites:
            if not loc in self.slots.keys() or not loc in self.sites.keys():
                logging.error('Found job for unknown site %s' %(loc))
                logging.error('ABORT: Am not processing jobGroup %i' %(wmbsJobGroup.id))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        myThread = threading.currentThread()

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
        logging.info(locations)

        #Prepare to get all jobs
        jobStates  = ['Created', 'Executing', 'SubmitFailed', 'JobFailed', 'SubmitCooloff', 'JobCooloff']

        #Get all jobs object
        jobFinder  = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerSite")
        for location in locations:
            value = int(jobFinder.execute(location = location, states = jobStates).values()[0])
            self.sites[location] = value
            logging.info("There are now %i jobs for site %s" %(self.sites[location], location))
            
        #You should now have a count of all jobs in self.sites

        return


    def submitJobs(self, jobList, localConfig = {}, subscription = None):
        """
        _submitJobs_
        
        This runs over the list of jobs and submits them all
        """

        myThread = threading.currentThread()

        changeState = ChangeState(self.config)

        pluginName = self.config.JobSubmitter.pluginName
        pluginDir  = self.config.JobSubmitter.pluginDir

        pluginPath = pluginDir + '.' + pluginName

        try:
            module      = __import__(pluginPath, globals(), locals(), [pluginName])
            instance    = getattr(module, pluginName)
            loadedClass = instance(self.config)
        except Exception, ex:
            msg = "Failed in attempting to import submitter module in JobSubmitter with msg = \n%s" %(ex)
            raise Exception(msg)

        successList, failList = loadedClass.submitJobs(jobList, localConfig)

        #Pass the successful jobs, and fail the bad ones
        myThread.transaction.begin()
        changeState.propagate(successList, 'executing',    'created')
        changeState.propagate(failList,    'SubmitFailed', 'Created')

        myThread.transaction.commit()

        return


    def grabTask(self, jobList):
        """
        _grabTask_

        Grabs the task, sandbox, etc for each job by using the WMBS DAO object
        """

        myThread = threading.currentThread()

        taskFinder = self.daoFactory(classname="Jobs.GetTask")

        #Assemble list
        jobIDs = []
        for job in jobList:
            jobIDs.append(job['id'])

        tasks = taskFinder.execute(jobID = jobIDs)

        taskDict = {}
        for job in jobList:
            #Now it gets interesting
            #Load the WMTask and grab the info that you need
            jobID = job['id']
            workloadName = tasks[jobID].split('/')[0]
            taskName = tasks[jobID].split('/')[1:]
            if type(taskName) == list:
                taskName = string.join(taskName, '/')
            #If we haven't picked this up before, pick it up now
            if not workloadName in taskDict.keys():
                #We know the format that the path is in
                workloadPath = os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pcl' %(workloadName))
                wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
                if not os.path.isfile(workloadPath):
                    workloadPath = os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pckl' %(workloadName))
                    if not os.path.isfile(workloadPath):
                        logging.error("Could not find WMSpec file %s in path %s for job %i" %(workloadName, workloadPath, jobID))
                        continue
                wmWorkload.load(os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pcl' %(workloadName)))
                taskDict[workloadName] = wmWorkload
                

            task = taskDict[workloadName].getTask(taskName)
            if not hasattr(task.data.input, 'sandbox'):
                logging.error("Job %i has no sandbox!" %(jobID))
                continue
            job['sandbox'] = task.data.input.sandbox
            

        return jobList

    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)

#    def submitBossLite(self, jobList, jobGroupConfig):
#        """
#        _submitBossLite_
#
#        Submit to the BossLite interface
#        """
#
#        myThread = threading.currentThread()
#
#        job.load()
#        jobGroup = JobGroup(id = job["jobgroup"])
#        jobGroup.load()
#        
#        jobGroupID = job["jobgroup"]
#        jobGroupConfig = self.getConfig(jobGroupConfig, subscription)
#
#        print "Have config"
#
#        BLconfig  = self.makeBossLiteConfig()
#        BLSconfig = self.makeBossLiteSchedConfig()
#
#        BLAPI       = BossLiteAPI(myThread.dialect, BLconfig)
#
#        BLAPI.connect()
#
#        jobConfig = {"jobId" : None,
#                     "taskId" : None,
#                     "name" : None
#                     }
#
#        print "Done BLAPI"
#
#        #Create a bossLite task
#
#        blTask = BLiteTask()
#        blTask['name']          = jobGroup.exists()
#        blTask['globalSandbox'] = jobGroupConfig['globalSandbox']
#        blTask['jobType']       = jobGroupConfig['jobType']
#        #BLAPI.saveTask(blTask)
#
#    
#        #Now I think here comes the hard part.
#        #For each WMBS job, we need to submit a BossLite job
#
#        for wmbsJob in jobGroup.getJobs():
#            #Okay, we have a job
#            outdir = time.strftime('%Y%m%d_%H%M%S')
#
#            wmbsJob.loadData()
#            jobConfig["name"]  = wmbsJob["name"]
#            jobConfig["jobId"] = wmbsJob["id"]
#            jobConfig["standardOutput"] = jobGroupConfig["stdOut"] %(wmbsJob["id"])
#            jobConfig["standardError"]  = jobGroupConfig["stdErr"] %(wmbsJob["id"])
#            BLjob = BLiteJob(jobConfig)
#
#            BLAPI.getNewRunningInstance( BLjob )
#
#            job.runningJob['outputDirectory'] = os.path.join(jobGroupConfig['jobCacheDir'] + outdir)
#
#            blTask.addJob(job)
#
#        print "Done creating %i jobs" %len(blTask.jobs)
#
#        BLAPI.updateDB(blTask)
#
#        logging.info( "Successfully Created task %s with %d jobs" % \
#                      ( blTask['id'], len(blTask.jobs) ) )
#
#
#
#        BLScheduler = BossLiteAPISched(BLAPI, BLSconfig)
#
#        BLScheduler.submit(taskId = blTask, jobRange = 'all')
#
#        changer.propagate(jobGroup.jobs, 'Executing', 'Created')
#
#        logging.info('If this worked, you would have a submitted job by now')
#
#
#        return
#
#
#    def submitJobs(self, jobGroup, jobGroupConfig, subscription = None):
#        """
#        This actually runs and submits jobs
#
#        """
#
#        myThread = threading.currentThread()
#
#        logging.info("Arrived in submitJobs")
#
#        changer = ChangeState(self.config)
#
#        logging.info("Have figured out how to changeState")
#
#        #if subscription is None:
#        jobGroup.loadData()
#        subscription = jobGroup.subscription
#
#        logging.info("Have subscription")
#
#        jobGroupID     = jobGroup.exists()
#
#        jobGroupConfig = self.getConfig(jobGroupConfig, subscription)
#
#        print "Have config"
#
#
#        BLconfig  = self.makeBossLiteConfig()
#        BLSconfig = self.makeBossLiteSchedConfig()
#
#        BLAPI       = BossLiteAPI(myThread.dialect, BLconfig)
#
#        BLAPI.connect()
#
#        jobConfig = {"jobId" : None,
#                     "taskId" : None,
#                     "name" : None
#                     }
#
#        print "Done BLAPI"
#
#        #print myThread.dbi.processData('SELECT * FROM bl_task', {})[0].fetchall()
#
#        #Create a bossLite task
#
#        blTask = BLiteTask()
#        blTask['name']          = jobGroup.exists()
#        blTask['globalSandbox'] = jobGroupConfig['globalSandbox']
#        blTask['jobType']       = jobGroupConfig['jobType']
#        #BLAPI.saveTask(blTask)
#
#    
#        #Now I think here comes the hard part.
#        #For each WMBS job, we need to submit a BossLite job
#
#        for wmbsJob in jobGroup.getJobs():
#            #Okay, we have a job
#            outdir = time.strftime('%Y%m%d_%H%M%S')
#
#            wmbsJob.loadData()
#            jobConfig["name"]  = wmbsJob["name"]
#            jobConfig["jobId"] = wmbsJob["id"]
#            jobConfig["standardOutput"] = jobGroupConfig["stdOut"] %(wmbsJob["id"])
#            jobConfig["standardError"]  = jobGroupConfig["stdErr"] %(wmbsJob["id"])
#            job = BLiteJob(jobConfig)
#
#            BLAPI.getNewRunningInstance( job )
#
#            job.runningJob['outputDirectory'] = os.path.join(jobGroupConfig['jobCacheDir'] + outdir)
#
#            blTask.addJob(job)
#
#        print "Done creating %i jobs" %len(blTask.jobs)
#
#        BLAPI.updateDB(blTask)
#
#        logging.info( "Successfully Created task %s with %d jobs" % \
#                      ( blTask['id'], len(blTask.jobs) ) )
#
#
#
#        BLScheduler = BossLiteAPISched(BLAPI, BLSconfig)
#
#        BLScheduler.submit(taskId = blTask, jobRange = 'all')
#
#        changer.propagate(jobGroup.jobs, 'Executing', 'Created')
#
#        logging.info('If this worked, you would have a submitted job by now')
#
#
#        return
#
#
#        
#
#
#    def makeBossLiteConfig(self):
#        """
#        Makes a config for the BossLiteAPI
#
#        """
#
#        myThread = threading.currentThread()
#
#        database = self.config.CoreDatabase.connectUrl
#
#        if myThread.dialect.lower() == 'mysql':
#            BLHost   = database.split('@')[1].split(':')[0]
#            BLUser   = database.split(':')[1].strip('//')
#            BLPass   = database.split(':')[2].split('@')[0]
#            BLName   = database.split('/')[-1]
#            BLPort   = database.split('@')[1].split(':')[1].split('/')[0]
#        else:
#            BLHost   = 'localhost'
#            BLUser   = 'mnorman'
#            BLPass   = ''
#            BLName   = database.split('///')[1]
#            BLPort   = ''
#
#
#        BLconfig = {'dbName':BLName,
#                    'host':BLHost,
#                    'user':BLUser,
#                    'passwd':BLPass,
#                    'socketFileLocation':self.config.CoreDatabase.dbsock,
#                    'portNr':BLPort,
#                    'refreshPeriod' : 4*3600 ,
#                    'maxConnectionAttempts' : 5,
#                    'dbWaitingTime' : 10
#                    }
#
#
#        return BLconfig
#
#
#    def makeBossLiteSchedConfig(self):
#        """
#        Makes a config for the BossLiteSchedAPI
#
#        """
#
#        BLSconfig = self.schedulerConfig
#        BLSconfig['user_proxy'] = '/proxy/path'
#        BLSconfig['service']    = 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server'
#        BLSconfig['config']     = '/etc/glite_wms.conf'
#
#        #BLSconfig = {'name' : 'SchedulerGLiteAPI',
#        #             'user_proxy' : '/proxy/path',
#        #             'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
#        #             'config' : '/etc/glite_wms.conf' }
#
#        return BLSconfig
#
#
#
#    def getConfig(self, config, subscription):
#        """
#        This fills the config with information from the WMSpec, if possible
#
#        """
#
#        #First, I will set the essential variables that have to be set.
#        #I will set them to test parameters
#        if not config.has_key('globalSandbox'):
#            config['globalSandbox'] = os.getcwd() + '/tmp/'
#        if not config.has_key('jobType'):
#            config['jobType'] = 'test'
#        if not config.has_key('jobCacheDir'):
#            config['jobCacheDir'] = config['globalSandbox']
#        if not config.has_key('stdOut'):
#            config['stdOut'] = 'Job_%s.out'
#        if not config.has_key('stdErr'):
#            config['stdErr'] = 'Job_%s.err'
#
#        subscription.loadData()
#        workflow = subscription['workflow']
#        workflow.load()
#        wmSpec = workflow.spec
#
#        #Without a WMSpec, we assume this is a test job, and return that config
#        if not os.path.isfile(wmSpec):
#            return config
#
#
#        #If we have the config, things get tricky
#        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
#        wmWorkload.load(wmSpec)
#
#        #Okay, now we have a spec.  But we NEED a task.
#        if not workflow.task:
#            return config
#
#        if not workflow.task in wmWorkload.listAllTaskNames():
#            return config
#
#        task = wmWorkload.getTask(workflow.task)
#
#        config['jobType'] = task.name
#        if hasattr(task.data.input, 'sandbox'):
#            config['globalSandbox'] = task.data.input.sandbox
#        if hasattr(task.data.input, 'jobCache'):
#            config['jobCacheDir'] = task.data.input.jobCache
#        if hasattr(task.data.input, 'stdOut'):
#            config['stdOut'] = task.data.input.stdOut
        


        




