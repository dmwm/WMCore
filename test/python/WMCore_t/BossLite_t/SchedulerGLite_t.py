#!/usr/bin/env python
"""
_SchedulerGLite_t_

"""




import unittest
import time
import nose
        
# Import key features
from WMQuality.TestInit import TestInit

# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
# from WMCore.BossLite.DbObjects.RunningJob  import RunningJob
from WMCore.BossLite.Common.Exceptions import SchedulerError

# Import API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched

from WMCore.BossLite.Common.System import executeCommand
from nose.plugins.attrib import attr

def fakeTask(db, numjob):
    """
    This procedure create a fakeTask
    """

    taskParams = {'name' : 'testTask',
                  'globalSandbox' : '/etc/redhat-release' }

    jobParams = {'executable' : '/bin/hostname',
                 'arguments' : '-f' }


    task = Task(taskParams)
    task.create(db)
    # self.assertEqual(tmpId, task.exists(db))
    task.exists(db)
    i = 0
    for j in xrange(numjob):
        
        jobParams['name'] = 'Fake_job_%s' % str(j)
        jobParams['standardError'] = 'hostname-%s.err' % str(j)
        jobParams['standardOutput'] = 'hostname-%s.out' % str(j)
        jobParams['outputFiles'] = [ jobParams['standardOutput'] ]
        jobParams['wmbsJobId']   = i
        i += 1
        
        job = Job( parameters = jobParams )
        job.newRunningInstance(db)
        task.addJob(job)
        
    task.save(db)
    
    return


def printDebug(task, runningInstance = False):
    """
    printDebug
    """
    
    if not runningInstance :
        msg = '\n >> schedulerParentId: ' + \
                    str(task.jobs[0].runningJob['schedulerParentId']) + '\n' 
        print msg
    
    msg = 'schedulerId'.ljust(52) + 'status'.ljust(6) + \
                'statusScheduler'.ljust(20) + 'destination'.ljust(20)
    print msg
    
    msg = ('-'*52).ljust(52) + ('-'*6).ljust(6) + \
                ('-'*20).ljust(20) + ('-'*20).ljust(20)
    print msg
    
    for job in task.jobs :
        if job.runningJob is None :
            msg = str(job.runningJob['schedulerId']).ljust(52) + \
                    'No running job'.rjust(10)
            print msg
            
        else :
            msg = str(job.runningJob['schedulerId']).ljust(52) + \
                     str(job.runningJob['status']).ljust(6) + \
                     str(job.runningJob['statusScheduler']).ljust(20) + \
                     str(job.runningJob['destination']).ljust(20)
            
            print msg


class SchedulerGLite(unittest.TestCase):
    """
    Unit-test for BossLiteAPISched
    """
    
    numjob = 8
    stoppingCriteria = 4
    toKill = 3


    def setUp(self):
        testInit = TestInit(__file__)
        testInit.setLogging()
        testInit.setDatabaseConnection()

        testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)

    def tearDown(self):
        testInit = TestInit(__file__)
        testInit.setLogging()
        testInit.setDatabaseConnection()
        
        testInit.clearDatabase()        

    def testA_databaseStartup(self):
        """
        testA_databaseStartup
        """
        raise nose.SkipTest
        # populate DB
        myBossLiteAPI = BossLiteAPI()
        fakeTask(myBossLiteAPI.db, numjob = self.numjob )
        
        return

    @attr('integration')        
    def testB_submission(self):
        """
        Simple submission operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedConfig =  {'name' : 'SchedulerGLite' }
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = mySchedConfig )
        
        try:
            task = mySchedAPI.submit( taskId = 1 )
        except SchedulerError, se:
            if se.value.find('Proxy') != -1:
                print 'WARNING: test does not fail but a proxy has not been found!!!'
                return
            else:
                raise se

        
        self.assertEqual(task['id'], 1)
              
        return
    
    
    @attr('integration')        
    def testC_status(self):
        """
        Simple status check operation
        """
        raise nose.SkipTest
        myBossLiteAPI = BossLiteAPI()
        mySchedConfig =  {'name' : 'SchedulerGLite' }
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = mySchedConfig )
        
        # polling status... the test ends when all jobs reach the status 'SD'
        while True:

            try:
                task = mySchedAPI.query( taskId = 1 )
            except SchedulerError, se:
                if se.value.find('Proxy') != -1:
                    print 'WARNING: test does not fail but a proxy has not been found!!!'
                    return
                else:
                    raise se

            
            #### DEBUG ####
            printDebug(task)
            
            exitCondition = 0
            
            for x in task.jobs :
                if x.runningJob['status'] == 'SD' :
                    exitCondition += 1
            
            if exitCondition > (self.stoppingCriteria - 1 ) :
                break
            
            # sleeping for a while...
            time.sleep(60)
        
        return
    
    @attr('integration')        
    def testD_getOutput(self):
        """
        Simple getOutput operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedConfig =  {'name' : 'SchedulerGLite' }
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = mySchedConfig )
        command = "mkdir ./test"
        executeCommand( command )
        
        extractedJob = myBossLiteAPI.loadEnded()
        
        if len(extractedJob) > 0 :
            jobRange = ','.join([str(x['jobId']) for x in extractedJob ])

            try:
                task = mySchedAPI.getOutput( taskId = 1, 
                                jobRange = jobRange, outdir = './test' )
            except SchedulerError, se:
                if se.value.find('Proxy') != -1:
                    print 'WARNING: test does not fail but a proxy has not been found!!!'
                    return
                else:
                    raise se

            
            #### DEBUG ####
            printDebug(task, runningInstance = True)
        
        return
    
    @attr('integration')
    def testD_kill(self):
        """
        Simple kill operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedConfig =  {'name' : 'SchedulerGLite' }
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = mySchedConfig )
        
        extractedJob = myBossLiteAPI.loadSubmitted(limit = self.toKill)
        
        if len(extractedJob) > 0 :
            jobRange = ','.join([str(x['jobId']) for x in extractedJob ])

            try:
                task = mySchedAPI.kill( taskId = 1, jobRange = jobRange)
            except SchedulerError, se:
                if se.value.find('Proxy') != -1:
                    print 'WARNING: test does not fail but a proxy has not been found!!!'
                    return
                else:
                    raise se
            
            #### DEBUG ####
            printDebug(task, runningInstance = True)
        
        return

if __name__ == "__main__":
    GliteSuite = unittest.TestLoader().loadTestsFromTestCase(SchedulerGLite)

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(GliteSuite)
