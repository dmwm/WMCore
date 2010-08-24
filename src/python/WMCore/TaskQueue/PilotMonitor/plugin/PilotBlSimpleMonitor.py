from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI

from JobSubmitter.JSException import JSException
from ProdAgent.Resources.LSF import LSFConfiguration
from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB

from PilotMonitor.plugin.PilotCalculationAlgorithm import runAlgorithm, runAlgorithm2,cmpf
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

    def __call__(self, destination=None):
        """
        _operator()_

        Query BossLite Here....

        """

        #sqlStr1 = \
        """
        select count(bl_job.id) from bl_job,bl_runningjob where bl_runningjob.job_id=bl_job.job_id and bl_runningjob.task_id=bl_job.task_id and bl_job.name like '%pilot%' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';
        """
        sqlStr1 = ''
       
        if ( destination!=None and destination.find('cern.ch')>0 ):  
            sqlStr1 = \
            """select count(bl_runningjob.id) from bl_task,bl_runningjob where bl_runningjob.task_id=bl_task.id and bl_task.job_type='PilotJob' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';
            """
        elif ( destination != None):
            sqlStr1 = \
            """select count(bl_runningjob.id) from bl_task,bl_runningjob where bl_runningjob.task_id=bl_task.id and bl_task.job_type='PilotJob' and bl_runningjob.destination='%s' and bl_runningjob.status not in ('E','C','A','SD') and bl_runningjob.closed='N';          """ % destination

        print sqlStr1

        bossLiteDB = BossLiteDB( 'MySQL', dbConfig )
        self.pilotJobs = bossLiteDB.selectOne( sqlStr1 )
        print self.pilotJobs
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
        """ 
        __monitorPilot__ 

        This fnction will submit pilot jobs using
        the selected mechanism  
        """
        try:
            poller = PABossLitePoll()

            siteValues = tqStateApi.getPilotCountsBySite()
            taskPacks  = tqStateApi.countTasksBySeReq()
            
            self.logger.debug('siteValues %s' % siteValues)
            self.logger.debug('taskPacks  %s' % taskPacks)
            self.logger.debug('__call__: %s' % site)
            self.logger.debug("has_key:%s" % siteValues.has_key(site) )
     
            if ( siteValues.has_key(site) ):
                self.logger.debug('got site %s' % site) 
                siteValue = siteValues[site]
            elif ( not siteValues.has_key(site) ):
                self.logger.debug('could not fin site/ so 0 values: %s' % site)
                siteValue = {'ActivePilots':0, 'IdlePilots':0}

            siteThr = self.siteThresholds[site]
            
            poller(site)
            totalSubmittedPilots = poller.pilotJobs
            result={}
            #result[site] = runAlgorithm(totalSubmittedPilots, siteThr)
            result[site] = runAlgorithm2(totalSubmittedPilots, site, siteThr, siteValue, taskPacks)
            self.logger.debug( result )
            self.logger.info("PJ RequiredJobs: %s" % result[site]['available'])

            return result 
        except:
            self.logger.debug( sys.exc_info()[0] )
            self.logger.debug( sys.exc_info()[1] )
            return {'Error':'ERROR'}
 
    # Monitor
    def monitorAll(self, tqStateApi):
        """
        _monitorAll_

        monitor all sites  
        """ 
        self.logger.debug('monitorAll')
        try:
            poller = PABossLitePoll()
            totalSubmittedPilots = poller.pilotJobs
            siteValues = tqStateApi.getPilotCountsBySite()  
            taskPacks = tqStateApi.countTasksBySeReq()
            #sort taskPacks collection 
            taskPacks.sort(cmpf)

            self.logger.debug('siteValues %s' % siteValues)
            self.logger.debug('taskPacks  %s' % taskPacks)
 

            result={}   
            #self.logger.debug(self.siteThresholds) 
            for siteSE in self.siteThresholds.keys():
                #do nothing for fake=TASK_QUEUE 
                if ( siteSE=='fake' ): 
                    continue
 
                siteThr = self.siteThresholds[siteSE]

                self.logger.debug('siteSE: %s' % siteSE)
                self.logger.debug('siteThr: %s' % siteThr)

                if ( siteValues.has_key(siteSE) ):
                    siteValue = siteValues[siteSE]
                elif ( siteValues.has_key(siteSE) == False ):
                    siteValue = { 'ActivePilots':0, 'IdlePilots':0}

                result[siteSE] = runAlgorithm2 ( totalSubmittedPilots, siteSE, \
                                       siteThr, siteValue, taskPacks )
 
            return result

        except:           
            self.logger.debug(sys.exc_info()[0])
            self.logger.debug(sys.exc_info()[1])
            return {'Error':'ERROR'} 
             
registerMonitor(PilotBlSimpleMonitor, PilotBlSimpleMonitor.__name__)

#for testing purpose
if __name__ == '__main__':
   args = {'cpCmd':'rfcp','rfioSer':''}
   from WMCore import Configuration
   tqconfig = Configuration.loadConfigurationFile( \
                   "/data/khawar/antonio/tqcode/extras/WMCore-conf.py" )
   from TQComp.Apis.TQStateApi import TQStateApi
   myThread = threading.currentThread()
   logger = logging.getLogger()
   #self.logger.debug( tqconfig)
   tqStateApi = TQStateApi(logger, tqconfig, None)

   pbsched = PilotBlSimpleMonitor()
   pbsched.monitorAll(tqStateApi)
   #pbsched('srm-cms.cern.ch')
   #poller = PABossLitePoll()
   #poller('abc.it.fr')
   #pbsched.getGroups(None)
  
