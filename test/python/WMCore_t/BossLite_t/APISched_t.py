#!/usr/bin/env python
"""
_APISched_t_

"""

__revision__ = "$Id: APISched_t.py,v 1.1 2010/05/24 14:14:52 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
        
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

    jobParams = {'executable' : '/bin/echo',
                 'arguments' : 'ciao',
                 'standardError' : 'err.txt',
                 'standardOutput' : 'out.txt',
                 'outputFiles' : ['out.txt'] }


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

class APISched(unittest.TestCase):
    """
    Unit-test for BossLiteAPISched
    """
    
    def setUp(self):
        """
        _setUp_
        """
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)
        
        return
    
    
    def tearDown(self):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return

    
    def testA_SubmissionGLite(self):
        """
        Simple submission through SchedulerGLite
        """
        
        myBossLiteAPI = BossLiteAPI()
        
        mySchedConfig =  {'name' : 'SchedulerGLite' }
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       
                                       schedulerConfig = mySchedConfig )

        fakeTask(myBossLiteAPI.db, numjob= 5)
        
        task = mySchedAPI.submit( 1 )
        
        
        #### DEBUG ####
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
        
        return
    

if __name__ == "__main__":
    APISchedSuite = unittest.TestLoader().loadTestsFromTestCase(APISched)

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(APISchedSuite)
