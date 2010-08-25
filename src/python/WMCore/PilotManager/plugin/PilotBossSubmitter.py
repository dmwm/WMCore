from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Scheduler.SchedulerLsf import SchedulerLsf
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.Job import Job

#Boss Errros
from ProdCommon.BossLite.Common.Exceptions import TaskError
from ProdCommon.BossLite.Common.Exceptions import JobError
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

from JobSubmitter.JSException import JSException

import traceback
import socket
import time
import sys
import os

class PilotBossSubmitter:

    def __init__(self, schedType, **args):
        """ __init__ """

        self.host = socket.getfqdn()
        self.schedType = schedType
	self.bossTask = None
        self.scheduler = None
        print dbConfig 
        self.bossLiteSession = BossLiteAPI('MySQL', dbConfig)
   
    def submitPilot(self, taskName, exePath, exe, inpSandbox):
        """ __submitPilot__ """

	if ( self.schedType == 'LSF' ):
	    #schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
            schedConfig = {'cpCmd': 'rfcp', 'rfioSer': '%s:' % self.host}
	    
	    self.scheduler = Scheduler.Scheduler('SchedulerLsf', schedConfig)
	    #self.scheduler = SchedulerLsf(cpCmd='rfcp', rfioSer='vocms13.cern.ch:')
	    inpSandbox = 'data/khawar/prototype/Pilot.tar'
	    exe = 'pilotbossSub.sh'
	    exePath = 'data/khawar/prototype/work'
	    self.lsfPilotTask(taskName, exe, exePath, inpSandbox)
        

    def lsfPilotTask(self, taskName, exe, exePath, inpSandbox):
        """ 
	__lsfPilotTask__
	
	declares Task and Job objects but don't store
	in the db
	"""
	        
        outdir = time.strftime('%Y%m%d_%H%M%S')
        lsfLogDir = os.path.join( '/afs/cern.ch/user/k/khawar/scratch2/khawar/logs', outdir )
        
	#task object
        try:
            os.mkdir(lsfLogDir)

            self.bossTask = Task()
            self.bossTask['name'] = taskName
            self.bossTask['globalSandbox']= exePath+'/'+exe+','+ inpSandbox
            self.bossTask['jobType'] = 'Processing'
            self.bossTask['outputDirectory']=lsfLogDir 
            self.bossLiteSession.saveTask( self.bossTask )
        except BossLiteError, ex:
            self.failedSubmission = 'pilotboss' 
            raise JSException(str(ex), FailureList = self.failedSubmission)
	
	#job object 
        try:
            job = Job()
            job['name'] = 'pilot' 
            job['arguments'] =  'pilot'
            #job['inputFiles'] = 'data/khawar/prototype/Pilot.tar'
            job['standardOutput'] =  'pilot.log'
            job['standardError']  =  'piloterr.log'
            job['executable']     =  exe
            job['outputFiles'] = [ 'pilot.log', \
                                   'pilot.tgz', \
                                   'FrameworkJobReport.xml' ]
            self.bossLiteSession.getNewRunningInstance( job )
            self.bossTask.addJob( job )
            self.bossLiteSession.updateDB( self.bossTask )
	    #get queue information from config
            requirements = '-q 8nh80' 
            #map, taskId, queue = self.scheduler.submitJob(job, self.bossTask, requirements)
            print self.scheduler.submit(self.bossTask, requirements)
            #print map
            #print 'taskId: %s'%taskId
            #print 'queue: %s'%queue 

        except: 
            print 'error: %s' % sys.exc_info()[0]
            print '%s:%s' % (sys.exc_info()[1], sys.exc_info()[2])
            traceback.print_exc(file=sys.stdout)
            raise Exception('pilotsubmissionfailed','pilot submission failed')


#for testing purpose
if __name__ == '__main__':
   args = {'cpCmd':'rfcp','rfioSer':''}
   pbsched = PilotBossSubmitter('LSF')
   pbsched.submitPilot('PilotJob','','','')
