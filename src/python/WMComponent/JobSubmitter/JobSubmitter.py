
#!/usr/bin/env python

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitter.py,v 1.1 2009/07/09 22:17:23 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import logging
import threading
import time
import os.path
#import common

#WMBS objects
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job as WMBSJob
from WMCore.WMBS.Workflow     import Workflow
from WMCore.DAOFactory        import DAOFactory

from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                     import WMTask, WMTaskHelper


from ProdCommon.BossLite.API.BossLiteAPI      import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.DbObjects.Job        import Job as BLiteJob
from ProdCommon.BossLite.DbObjects.Task       import Task as BLiteTask

from WMCore.JobStateMachine.ChangeState import ChangeState

class JobSubmitter(object):
    """
    Handles job submission

    """

    def __init__(self, config):

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        #Dictionary definitions
        self.locations = {}

        #Set config objects
        self.database = config.CoreDatabase.connectUrl
        self.dialect  = config.CoreDatabase.dialect

        self.session = None
        self.schedulerConfig = {}
        self.cfg_params = None
        self.config = config

        return


    def configure(self,cfg_params):
        
        myThread = threading.currentThread()


        self.cfg_params = cfg_params
        if not self.cfg_params.has_key('scheduler'):
            self.cfg_params['scheduler'] = 'fake'

        
        #self.deep_debug= self.cfg_params.get("USER.deep_debug",'0')
        #server_check =  self.cfg_params.get("CRAB.server_name",None)
        #if self.deep_debug == '1' and server_check != None :
        #    msg =  'You are asking the deep_debug, but it cannot works using the server.\n'
        #    msg += '\t The functionality will not have effect.'
        #    logging.info(msg) 
        #self.schedulerName =  self.cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements
        #self.rb_param_file=''
        if not self.cfg_params.has_key('GRID.rb'):
            self.cfg_params['GRID.rb']='CERN'
        #self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("GRID.rb"))
        #self.wms_service=cfg_params.get("GRID.wms_service",'')

        #self.wrapper = cfg_params.get('CRAB.jobtype').upper()+'.sh'


        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
        SchedMap = {'glite':    'SchedulerGLiteAPI',
                    'glitecoll':'SchedulerGLiteAPI',\
                    'condor':   'SchedulerCondor',\
                    'condor_g': 'SchedulerCondorG',\
                    'glidein':  'SchedulerGlidein',\
                    'lsf':      'SchedulerLsf',\
                    'caf':      'SchedulerLsf',\
                    'sge':      'SchedulerSge',
                    'arc':      'SchedulerARC',
                    'fake':     'SchedulerFake'
                    }

        #self.schedulerConfig = common.scheduler.realSchedParams(cfg_params)
        self.schedulerConfig['name'] =  SchedMap[(self.cfg_params['scheduler']).lower()]
        self.schedulerConfig['timeout'] = 180
        self.schedulerConfig['skipProxyCheck'] = True 
        self.schedulerConfig['logger'] = myThread.logger

        self.session = None
        return



    def setLocations(self):
        self.locations = {}

        #Then get all locations
        locationList            = self.daoFactory(classname = "Locations.List")
        locID, locations, slots = locationList.execute()

        for location in locations:
            self.locations[location] = []


    def submitJobs(self, jobGroup, jobGroupConfig, subscription = None):
        """
        This actually runs and submits jobs

        """

        myThread = threading.currentThread()




        #changer = ChangeState()

        #if subscription is None:
        jobGroup.loadData()
        subscription = jobGroup.subscription

        logging.info("Entered submitJobs")

        jobGroupID     = jobGroup.exists()

        jobGroupConfig = self.getConfig(jobGroupConfig, subscription)

        return

        BLconfig  = self.makeBossLiteConfig()
        BLSconfig = self.makeBossLiteSchedConfig()

        BLAPI       = BossLiteAPI(myThread.dialect, BLconfig)

        BLAPI.connect()

        jobConfig = {"jobId" : None,
                     "taskId" : None,
                     "name" : None
                     }

        return

        #print myThread.dbi.processData('SELECT * FROM bl_task', {})[0].fetchall()

        #Create a bossLite task

        blTask = BLiteTask()
        blTask['name']          = jobGroup.exists()
        blTask['globalSandbox'] = jobGroupConfig['globalSandbox']
        blTask['jobType']       = jobGroupConfig['jobType']
        #BLAPI.saveTask(blTask)

    
        #Now I think here comes the hard part.
        #For each WMBS job, we need to submit a BossLite job

        for wmbsJob in jobGroup.getJobs():
            #Okay, we have a job
            outdir = time.strftime('%Y%m%d_%H%M%S')

            wmbsJob.loadData()
            jobConfig["name"]  = wmbsJob["name"]
            jobConfig["jobId"] = wmbsJob["id"]
            jobConfig["standardOutput"] = jobGroupConfig["stdOut"] %(wmbsJob["id"])
            jobConfig["standardError"]  = jobGroupConfig["stdErr"] %(wmbsJob["id"])
            job = BLiteJob(jobConfig)

            BLAPI.getNewRunningInstance( job )

            job.runningJob['outputDirectory'] = os.path.join(jobGroupConfig['jobCacheDir'] + outdir)

            blTask.addJob(job)

        BLAPI.updateDB(blTask)

        logging.info( "Successfully Created task %s with %d jobs" % \
                      ( blTask['id'], len(blTask.jobs) ) )



        BLScheduler = BossLiteAPISched(BLAPI, BLSconfig)

        BLScheduler.submit(taskId = blTask, jobRange = 'all')

        #changer.propagate(jobGroup.jobs, 'Created', 'New')

        logging.info('If this worked, you would have a submitted job by now')


        


    def makeBossLiteConfig(self):
        """
        Makes a config for the BossLiteAPI

        """

        myThread = threading.currentThread()

        database = self.config.CoreDatabase.connectUrl

        if myThread.dialect.lower() == 'mysql':
            BLHost   = database.split('@')[1].split(':')[0]
            BLUser   = database.split(':')[1].strip('//')
            BLPass   = database.split(':')[2].split('@')[0]
            BLName   = database.split('/')[-1]
            BLPort   = database.split('@')[1].split(':')[1].split('/')[0]
        else:
            BLHost   = 'localhost'
            BLUser   = 'mnorman'
            BLPass   = ''
            BLName   = database.split('///')[1]
            BLPort   = ''


        BLconfig = {'dbName':BLName,
                    'host':BLHost,
                    'user':BLUser,
                    'passwd':BLPass,
                    'socketFileLocation':self.config.CoreDatabase.dbsock,
                    'portNr':BLPort,
                    'refreshPeriod' : 4*3600 ,
                    'maxConnectionAttempts' : 5,
                    'dbWaitingTime' : 10
                    }


        return BLconfig


    def makeBossLiteSchedConfig(self):
        """
        Makes a config for the BossLiteSchedAPI

        """

        BLSconfig = self.schedulerConfig
        BLSconfig['user_proxy'] = '/proxy/path'
        BLSconfig['service']    = 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server'
        BLSconfig['config']     = '/etc/glite_wms.conf'

        #BLSconfig = {'name' : 'SchedulerGLiteAPI',
        #             'user_proxy' : '/proxy/path',
        #             'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
        #             'config' : '/etc/glite_wms.conf' }

        return BLSconfig



    def getConfig(self, config, subscription):
        """
        This fills the config with information from the WMSpec, if possible

        """

        #First, I will set the essential variables that have to be set.
        #I will set them to test parameters
        if not config.has_key('globalSandbox'):
            config['globalSandbox'] = os.getcwd() + '/tmp/'
        if not config.has_key('jobType'):
            config['jobType'] = 'test'
        if not config.has_key('jobCacheDir'):
            config['jobCacheDir'] = config['globalSandbox']
        if not config.has_key('stdOut'):
            config['stdOut'] = 'Job_%s.out'
        if not config.has_key('stdErr'):
            config['stdErr'] = 'Job_%s.err'

        subscription.loadData()
        workflow = subscription['workflow']
        workflow.load()
        wmSpec = workflow.spec

        #Without a WMSpec, we assume this is a test job, and return that config
        if not os.path.isfile(wmSpec):
            return config


        #If we have the config, things get tricky
        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
        wmWorkload.load(wmSpec)

        #Okay, now we have a spec.  But we NEED a task.
        if not workflow.task:
            return config

        if not workflow.task in wmWorkload.listAllTaskNames():
            return config

        task = wmWorkload.getTask(workflow.task)

        config['jobType'] = task.name
        if hasattr(task.data.input, 'sandbox'):
            config['globalSandbox'] = task.data.input.sandbox
        if hasattr(task.data.input, 'jobCache'):
            config['jobCacheDir'] = task.data.input.jobCache
        if hasattr(task.data.input, 'stdOut'):
            config['stdOut'] = task.data.input.stdOut
        


        




    
