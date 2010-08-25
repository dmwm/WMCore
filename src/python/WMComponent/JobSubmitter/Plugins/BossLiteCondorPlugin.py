#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, E1103
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# E1103: The thread will have a logger and a dbi before it gets here

"""
_CondorGlobusPlugin_

A plug-in that should submit directly to condor globus CEs

"""




import os
import os.path
import logging
import threading
import cPickle

import subprocess

from WMCore.DAOFactory import DAOFactory

from WMCore.WMInit import getWMBASE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase


# BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task


# BossLite API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched

from WMCore.Services.UUID import makeUUID


subprocess._cleanup = lambda: None



def parseError(error):
    """
    Do some basic condor error parsing

    """

    errorCondition = False
    errorMsg       = ''

    if 'ERROR: proxy has expired\n' in error:
        errorCondition = True
        errorMsg += 'CRITICAL ERROR: Your proxy has expired!'


    return errorCondition, errorMsg

class BossLiteCondorPlugin(PluginBase):
    """
    _CondorGlobusPlugin_
    
    A plug-in that should submit directly to condor globus CEs
    """

    def __init__(self, **configDict):

        PluginBase.__init__(self, config = configDict)
        
        self.config = configDict

        self.locationDict = {}

        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")

        self.packageDir = None
        self.unpacker   = None
        self.sandbox    = None

        return



    def __call__(self, parameters):
        """
        _submitJobs_
        
        If this class actually did something, this would handle submissions
        """

        if parameters == {} or parameters == []:
            return {'NoResult': [0]}

        result = {'Success': []}

        for entry in parameters:
            jobList         = entry.get('jobs')
            self.packageDir = entry.get('packageDir', None)
            index           = entry.get('index', 0)
            self.sandbox    = entry.get('sandbox', None)
            self.agent      = entry.get('agentName', 'test')
            self.unpacker   = os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')


            # Create a task and some jobs
            myBossLiteAPI = BossLiteAPI()

            db = myBossLiteAPI.db

            
            taskParams = {'name' : makeUUID(),
                          'globalSandbox' : self.sandbox,
                          'serverName': self.agent}


            task = Task(taskParams)
            task.create(db)
            task.exists(db)

            jpkgPath = os.path.join(self.packageDir, 'JobPackage.pkl')
            

            


            for job in jobList:
                jobParams = {'executable' : '%s' % self.config['submitScript'],
                             'arguments' : '%s %i' % (os.path.basename(self.sandbox),
                                                      index)}
                jobParams['name']           = job['name']
                jobParams['standardError']  = 'test-%s.err' % str(job['name'])
                jobParams['standardOutput'] = 'test-%s.out' % str(job['name'])
                jobParams['outputFiles']    = ['Report.pkl']
                jobParams['inputFiles']     = [jpkgPath, self.sandbox, self.unpacker]
                jobParams['jobId']          = job['id']
                jobParams['standardInput']  = job['cache_dir']


                index += 1

                job = Job( parameters = jobParams )
                job.newRunningInstance(db)
                task.addJob(job)

            task.save(db)



            
            mySchedConfig =  {'name' : 'SchedulerCondorG',
                              'tmpDir': jobList[0]['cache_dir']}
            
            requirements='globusscheduler = %s;' %(jobList[0]['custom']['location'])
            
            mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                           schedulerConfig = mySchedConfig)

            task = mySchedAPI.submit( taskId = 1,
                                      requirements = requirements )

            successList = []
            failList    = []

            for job in jobList:
                result['Success'].append(job['id'])
                job['couch_record'] = None
                successList.append(job)


            if len(successList) > 0:
                self.passJobs(jobList = successList)
            if len(failList) > 0:
                self.failJobs(jobList = failList)


            return result
