#!/usr/bin/env python
"""
_APISched_t_

"""

__revision__ = "$Id: APISched_t.py,v 1.2 2010/05/25 22:15:05 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import time
        
# Import key features
from WMQuality.TestInit import TestInit

# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
# from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

# Import API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched

def fakeTask(db, numjob):
    """
    This procedure create a fakeTask
    """

    taskParams = {'name' : 'testTask' }

    jobParams = {'executable' : '/bin/hostname',
                 'arguments' : '-f',
                 'standardError' : 'hostname.err',
                 'standardOutput' : 'hostname.out',
                 'outputFiles' : ['hostname.out'] }


    task = Task(taskParams)
    task.create(db)
    # self.assertEqual(tmpId, task.exists(db))
    task.exists(db)
    for j in xrange(numjob):
        jobParams['name'] = 'Fake_job_%s' % str(j)
        job = Job( parameters = jobParams )
        job.newRunningInstance(db)
        task.addJob(job)
        
    task.save(db)
    
    return


def printDebug(task):
    """
    printDebug
    """
    
    print '\n >> schedulerParentId: ' + \
                str(task.jobs[0].runningJob['schedulerId']) + '\n' 
    print 'schedulerId'.ljust(52), 'status'.ljust(6), \
                'statusScheduler'.ljust(20), 'destination'.ljust(20)
    print ('-'*52).ljust(52), ('-'*6).ljust(6), \
                ('-'*20).ljust(20), ('-'*20).ljust(20)
    for job in task.jobs :
        if job.runningJob is None :
            print str(job.runningJob['schedulerId']).ljust(52), \
                    'No running job'.rjust(10)
        else :
            print str(job.runningJob['schedulerId']).ljust(52), \
                     str(job.runningJob['status']).ljust(6), \
                     str(job.runningJob['statusScheduler']).ljust(20), \
                     str(job.runningJob['destination']).ljust(20)
        # print '\n'


class APISched(unittest.TestCase):
    """
    Unit-test for BossLiteAPISched
    """
    
    myBossLiteAPI = None
    mySchedConfig =  {'name' : 'SchedulerGLite' }
    
    def testA_databaseStartup(self):
        """
        testA_databaseStartup
        """
        
        testInit = TestInit(__file__)
        testInit.setLogging()
        testInit.setDatabaseConnection()
        
        testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)
        
        # populate DB
        self.myBossLiteAPI = BossLiteAPI()
        fakeTask(self.myBossLiteAPI.db, numjob= 3)
        
        return

    
    def testB_Submission(self):
        """
        Simple submission operation
        """
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = self.myBossLiteAPI, 
                                       schedulerConfig = self.mySchedConfig )
        
        task = mySchedAPI.submit( 1 )
        
        #### DEBUG ####
        printDebug(task)
        
        return
    
    
    
    def testC_Status(self):
        """
        Simple status check operation
        """
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = self.myBossLiteAPI, 
                                       schedulerConfig = self.mySchedConfig )
        
        # polling status... the test ends when all jobs reach the status 'SD'
        while True:
            
            task = mySchedAPI.query( 1 )
            
            #### DEBUG ####
            printDebug(task)
            
            exitCondition = True
            
            for x in task.jobs :
                if x.runningJob['status'] != 'SD' :
                    exitCondition = False
                    
            if exitCondition:
                break
            
            # sleeping for a while...
            time.sleep(60)
        
        return
    
    
    def testD_GetOutput(self):
        """
        Simple getOutput operation
        """
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = self.myBossLiteAPI, 
                                       schedulerConfig = self.mySchedConfig )
        
        # do something ...
        
        return
    
    
    def testD_Kill(self):
        """
        Simple kill operation
        """
        
        mySchedAPI = BossLiteAPISched( bossLiteSession = self.myBossLiteAPI, 
                                       schedulerConfig = self.mySchedConfig )
        
        # do something ...
        
        return
    
    
    def testZ_dropDatabase(self):
        """
        Simple submission through SchedulerGLite
        """
        
        testInit = TestInit(__file__)
        testInit.setLogging()
        testInit.setDatabaseConnection()
        
        testInit.clearDatabase()
        
        return
    
    
if __name__ == "__main__":
    APISchedSuite = unittest.TestLoader().loadTestsFromTestCase(APISched)

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(APISchedSuite)
