from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from JobTracking.TrackingDB import TrackingDB

from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job

#Boss Errros
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

from JobSubmitter.JSException import JSException
from ProdAgent.Resources.LSF import LSFConfiguration
from PilotMonitor.plugin.MonitorInterface import MonitorInterface

import traceback
import random
import logging
import socket
import time
import sys
import os

class PABossLitePoll:
    """
    _PABossLitePoll_
    """
    def __init__(self):
        self.pilotJobs = 0

    def __call__(self):
        """
        _operator()_

        Query BossLite Here....

        """
        ### config = loadProdAgentConfiguration()
        ### BOSSconfig = config.getConfig("BOSS")
        ### bossCfgDir = BOSSconfig['configDir']


        sqlStr1 = \
        """
        select count(bl_job.id) from bl_job,bl_runningjob where bl_runningjob.job_id=bl_job.job_id and bl_runningjob.task_id=bl_job.task_id and bl_job.name like '%pilot%' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';
        """
        bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
        pilotJobs = bossLiteDB.selectOne( sqlStr1 )
        #close and delete bossLiteDB
        #del bossLiteDB


class PilotBossMonitor(MonitorInterface):
    """ 
    _PilotBossMonitor_ 
    """
    def __init__(self, schedType, **args):
        """ __init__ """

        self.schedType = schedType
	self.bossTask = None
        self.scheduler = None
        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other'] 
        print dbConfig 
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sessionPool = self.bossLiteSession.bossLiteDB.getPool()
        self.db = TrackingDB( self.bossLiteSession.bossLiteDB )
    
    def monitorPilot(self, site, tqStateApi):
        """ __monitorPilot__ 
        This fnction will submit pilot jobs using
        the selected mechanism  
        """
         
	if ( self.schedType == 'LSF' ):
	    #schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
            schedConfig = {'cpCmd': 'rfcp', 'rfioSer': '%s:' % self.host}
	    #self.scheduler = Scheduler.Scheduler('SchedulerLsf', schedConfig)
            
            poller = PABossLitePoll()
            poller()
            total = poller.pilotJobs
            #start the logic here
 
    def getGroups(self, group):
        tasks = self.db.getGroupTasks(group)

        for taskId in tasks :        
            print taskId

    def getStatistic(self):
        """
        __getStatistics__

        Poll the BOSS DB for a summary of the job status

        """

        # summary of the jobs in the DB
        result = self.db.getJobsStatistic()

        if result is not None:

            counter = {}
            for ctr in self.counters:
                counter[ctr] = 0

            for pair in result :
                status, count = pair
                if status == 'E':
                    continue
                elif status == 'R' :
                    counter['running'] = count
                elif status == 'I':
                    counter['pending'] = count
                elif status == 'SW' :
                    counter['waiting'] = count
                elif status == 'SR':
                    counter['ready'] = count
                elif status == 'SS':
                    counter['scheduled'] = count
                elif status == 'SU':
                    counter['submitted'] = count
                elif status == 'SE':
                    counter['cleared'] = count
                elif status == 'C':
                    counter['created'] = count
                else:
                    counter['other'] += count

            # display counters
            for ctr, value in counter.iteritems():
                print(ctr + " jobs : " + str(value))
            print("....................")

            return result


    def pollJobs(self, runningAttrs, processStatus, skipStatus=None ):
        """
        __pollJobs__

        basic structure for jobs polling

        """

        offset = 0
        loop = True

        while loop :

            logging.debug("Max jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + self.jobLimit) ) )

            self.newJobs = self.bossLiteSession.loadJobsByRunningAttr(
                runningAttrs=runningAttrs, \
                limit=self.jobLimit, offset=offset
                )

            logging.info("Polled jobs : " + str( len(self.newJobs) ) )

            # exit if no more jobs to query
            if self.newJobs == [] :
                loop = False
                break
            else :
                offset += self.jobLimit

            try:
                self.db.processBulkUpdate( self.newJobs, processStatus, \
                                           skipStatus )
                logging.info( "Changed status to %s for %s loaded jobs" \
                              % ( processStatus, str( len(self.newJobs) ) ) )

            except BossLiteError, err:
                logging.error(
                    "Failed handling %s loaded jobs, waiting next round: %s" \
                    % ( processStatus, str( err ) ) )
                continue



#for testing purpose
if __name__ == '__main__':
   args = {'cpCmd':'rfcp','rfioSer':''}
   pbsched = PilotBossMonitor('LSF')
   pbsched.getStatistic()
   #pbsched.getGroups(None)
