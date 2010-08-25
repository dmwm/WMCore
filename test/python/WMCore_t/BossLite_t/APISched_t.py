#!/usr/bin/env python
"""
_APISched_t_

"""




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

from nose.plugins.attrib import attr

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


class APISched(unittest.TestCase):
    """
    Unit-test for BossLiteAPISched
    """
    
    @attr('integration')    
    def testA_databaseStartup(self):
        """
        testA_databaseStartup
        """
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        
        self.testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)
        
        # populate DB
        myBossLiteAPI = BossLiteAPI()
        fakeTask(myBossLiteAPI.db, numjob= 5)
        
        return
    
    @attr('integration')    
    def testB_Submission(self):
        """
        Simple submission operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = 
                                            { 'name' : 'SchedulerFake' } )
        
        # pass to APISched a valid Task object, not an ID
        myTask = myBossLiteAPI.loadTask( taskId = 1, jobRange='all' )
        taskLoaded = mySchedAPI.submit( taskObj = myTask )
        
        # pass to APISched an ID, not a Task object
        #taskLoaded = mySchedAPI.submit( taskId = 1 )
        
        for job in taskLoaded.jobs :
            self.assertEqual(job.runningJob['status'], 'S')
            self.assertEqual(job.runningJob['closed'], 'N')
        
        return
    
    @attr('integration')    
    def testC_Status(self):
        """
        Simple status check operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = 
                                            { 'name' : 'SchedulerFake' } )
        
        # pass to APISched a valid Task object, not an ID
        #myTask = myBossLiteAPI.loadTask( taskId = 1, jobRange='all' )
        #taskLoaded = mySchedAPI.query( taskObj = myTask )
        
        # pass to APISched an ID, not a Task object
        taskLoaded = mySchedAPI.query( taskId = 1 )
        
        for job in taskLoaded.jobs :
            self.assertEqual(job.runningJob['status'], 'SD')
            self.assertEqual(job.runningJob['closed'], 'N')
        
        return
    
    @attr('integration')    
    def testD_GetOutput(self):
        """
        Simple getOutput operation
        """
        
        myBossLiteAPI = BossLiteAPI()
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, 
                                       schedulerConfig = 
                                            { 'name' : 'SchedulerFake' } )
        
        # pass to APISched a valid Task object, not an ID
        myTask = myBossLiteAPI.loadTask( taskId = 1, jobRange='all' )
        taskLoaded = mySchedAPI.getOutput( taskObj = myTask, outdir = './test' )
        
        # pass to APISched an ID, not a Task object
        # taskLoaded = mySchedAPI.getOutput( taskId = 1, outdir = './test' )
        
        for job in taskLoaded.jobs :
            self.assertEqual(job.runningJob['status'], 'E')
            self.assertEqual(job.runningJob['closed'], 'Y')
             
        return
    
    
    ## TODO: use standard unit test behaviour
    def testE_dropDatabase(self):
        """
        Simple submission through SchedulerGLite
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        
        self.testInit.clearDatabase()
        
        return
    
    
if __name__ == "__main__":
    # Logging on external text file... not working
    #LOG_FILENAME = './APISched_t.log'
    #logging.basicConfig(filename=LOG_FILENAME)
    #logging.getLogger( "APISched_log" ).setLevel( logging.INFO )
    
    suiteSched = unittest.TestLoader().loadTestsFromTestCase(APISched)
    
    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(suiteSched)
