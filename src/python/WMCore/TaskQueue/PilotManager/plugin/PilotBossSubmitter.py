#!/usr/bin/env python

"""
_PilotBossSubmitter_

"""

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB
#from ProdCommon.BossLite.Scheduler.SchedulerLsf import SchedulerLsf
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job

#Boss Errros
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

from JobSubmitter.JSException import JSException
from ProdAgent.Resources.LSF import LSFConfiguration
from PilotManager.plugin.LSFInterface import loadLSFConfig
from PilotManager.plugin.Registry import registerManager

import threading
import traceback
import logging
import random
import socket
import time
import sys
import os

class PilotBossSubmitter:

    def __init__(self):
        """ __init__ """
        self.host = socket.getfqdn()
        self.schedType = None
	self.bossTask = None
        self.scheduler = None
        myThread = threading.currentThread()
        self.logger = myThread.logger  
        #self.logger.debug( dbConfig )
        #self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)

    def __call__(self, schedType):
        self.schedType = schedType
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)

    def taskCount (self):
        """
        __taskCount__ 

        count the tasks in the table.
        helps in making unique name for tasks
        """

        sql="select count(id) from bl_task"
        bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
        taskTotal = bossLiteDB.selectOne( sql ) 
        return taskTotal

    def submitPilot(self, taskName, exe, exePath, inpSandbox, bulkSize=1):
        """ __submitPilot__ 
        This fnction will submit pilot jobs using
        the selected mechanism  
        """

	if ( self.schedType == 'LSF' ):
	    #schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
            #schedConfig = {'cpCmd': 'rfcp', 'rfioSer': '%s:' % self.host}
            schedConfig = {'cpCmd': 'rfcp', 'rfioSer': '%s:' % self.host}
	    self.scheduler = Scheduler.Scheduler('SchedulerLsf', schedConfig)
	    self.lsfPilotTask(taskName, exe, exePath, inpSandbox, bulkSize)
        

    def lsfPilotTask(self, taskName, exe, exePath, inpSandbox, bulkSize):
        """ 
	__lsfPilotTask__
	
	declares Task and Job objects but don't store
	in the db
 	"""
        #load LSF configurations 
	lsfConfig = loadLSFConfig()       
        if ( lsfConfig is None):
            logging.debug('LSFConfig is None: returning back the call ')
            return
        
        outdir = time.strftime('%Y%m%d_%H%M%S')
        #got logDir information from the config
        logDir = lsfConfig['logDir'] 
        lsfLogDir = os.path.join(logDir, outdir )
          
        taskCount = self.taskCount()
        taskName = "%s_%s" % (taskName, taskCount + 1)
	#task object
        try:
            os.mkdir(lsfLogDir)
           
            self.bossTask = Task()
            self.bossTask['name'] = taskName
            self.bossTask['jobType'] = 'PilotJob'
            self.bossTask['globalSandbox'] = exePath+'/'+exe+','+ inpSandbox
            self.bossTask['outputDirectory'] = lsfLogDir 
            self.bossLiteSession.saveTask( self.bossTask )

        except BossLiteError, ex:
            self.failedSubmission = 'pilotboss' 
            raise JSException(str(ex), FailureList = self.failedSubmission)
	
	#job object 
        try:
            for j in range(0, bulkSize): 
                job = Job()
                job['name'] = '%s_pilot'%j 
                #these arguments are passed to the submission script
                # ANTO: job must output to its working dir
                #       LSF will copy result back to logDir
                #       Get std.out and std.err
#                job['standardOutput'] =  '%s/pilot_%s.log'%(lsfLogDir, j)
#                job['standardOutput'] =  'pilot_%s.log' % (j)
                job['standardOutput'] =  '%s_std.out' % j
#                job['standardError']  =  '%s/piloterr_%s.log'%(lsfLogDir, j)
#                job['standardError']  =  'piloterr_%s.log' % (j)
                job['standardError']  =  '%s_std.err' % j
                job['executable']     =  exe
#                job['outputFiles'] = [ '%s/pilot_%s.log'%(lsfLogDir, j), \
#                                       '%s/pilot_%s.tgz'%(lsfLogDir, j)]
#                job['outputFiles'] = [ 'pilot_%s.log' % (j), \
#                                       'pilot_%s.tgz' % (j)]
                job['outputFiles'] = [ '*std.out', '*std.err']
#                job['outputDirectory'] =  lsfLogDir + '/%s' % (j)

                self.bossLiteSession.getNewRunningInstance( job )
#                job.runningJob['outputDirectory'] =  lsfLogDir + '/%s' % (j)
#                os.mkdir(job.runningJob['outputDirectory'])
                # END ANTO
                self.bossTask.addJob( job )
                self.bossLiteSession.updateDB( self.bossTask )

            lsfQueue = lsfConfig['Queue'] 
            lsfRsrReq = lsfConfig['Resource']
            requirements = '-q %s -g %s' % (lsfQueue, LSFConfiguration.getGroup() )
            requirements += ' -J %s' % taskName
            if ( lsfRsrReq is not None or lsfRsrReq != ""):
                requirements += " -R \"%s\"" % lsfRsrReq

            self.logger.debug( 'Now Submitting %s pilotjobs through BossInterface' % bulkSize)
            output = self.scheduler.submit(self.bossTask, requirements)
            self.logger.debug(output) 

            self.bossLiteSession.updateDB( self.bossTask ) 

        except: 
            self.logger.debug( 'error: %s' % sys.exc_info()[0])
            self.logger.debug( '%s:%s' % (sys.exc_info()[1], sys.exc_info()[2]) )
            traceback.print_exc(file=sys.stdout)
            raise Exception('pilotsubmissionfailed','pilot submission failed')


#for testing purpose
if __name__ == '__main__':
   args = {'cpCmd':'rfcp','rfioSer':''}
   pbsched = PilotBossSubmitter('LSF')
   pbsched.submitPilot('PilotJob','','','')

registerManager(PilotBossSubmitter, PilotBossSubmitter.__name__)

