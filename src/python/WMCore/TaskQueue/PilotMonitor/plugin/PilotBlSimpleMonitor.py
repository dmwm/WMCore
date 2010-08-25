from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI

from JobSubmitter.JSException import JSException
from ProdAgent.Resources.LSF import LSFConfiguration
from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB

from PilotMonitor.plugin.PilotCalculationAlgorithm import runAlgorithm
from PilotMonitor.plugin.MonitorInterface import MonitorInterface
from PilotMonitor.plugin.Registry import registerMonitor

import traceback
import threading
import logging
import sys

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

        #sqlStr1 = \
        """
        select count(bl_job.id) from bl_job,bl_runningjob where bl_runningjob.job_id=bl_job.job_id and bl_runningjob.task_id=bl_job.task_id and bl_job.name like '%pilot%' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';
        """
        sqlStr1 = \
        """
        select count(bl_runningjob.id) from bl_task,bl_runningjob where bl_runningjob.task_id=bl_task.id and bl_task.job_type like '%pilot%' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';
        """
        bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
        self.pilotJobs = bossLiteDB.selectOne( sqlStr1 )
        #close and delete bossLiteDB
        #del bossLiteDB

class PilotBlSimpleMonitor(MonitorInterface):
    """ 
    _PilotBlSimpleMonitor_ 
    """
    def __init__(self):
        """ __init__ """
        MonitorInterface.__init__(self)
        myThread = threading.currentThread()
        self.logger = myThread.logger
    
    def __call__(self, site, tqStateApi):
        """ __monitorPilot__ 
        This fnction will submit pilot jobs using
        the selected mechanism  
        """
        print '_call_'    
        try:
            siteValues = tqStateApi.getPilotCountsBySite()
            taskPacks  = tqStateApi.countTasksBySeReq()
            self.logger.debug('siteValiues %s' % siteValues)
            self.logger.debug('taskPacksss %s' % taskPacks)

            siteThr = self.siteThresholds[site]
            self.logger.debug('site thrshlod %s' % siteThr )
            poller = PABossLitePoll()
            poller()
            totalSubmittedPilots = poller.pilotJobs
            result = runAlgorithm(totalSubmittedPilots, siteThr)
            self.logger.debug( result )
            self.logger.info("PJ RequiredJobs: %s" % result['available'])

            return result 
        except:
            self.logger.debug( sys.exc_info()[0] )
            self.logger.debug( sys.exc_info()[1] )
            return {'Error':'ERROR'}
            #start the logic here
 

registerMonitor(PilotBlSimpleMonitor, PilotBlSimpleMonitor.__name__)

#for testing purpose
if __name__ == '__main__':
   args = {'cpCmd':'rfcp','rfioSer':''}
   pbsched = PilotBlSimpleMonitor()
   pbsched('CERN')
   #pbsched.getGroups(None)
