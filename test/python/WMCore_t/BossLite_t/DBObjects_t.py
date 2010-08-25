#!/usr/bin/env python

import unittest
import threading
import string
import time
import logging

# Import key features
from WMQuality.TestInit import TestInit

# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM
from WMCore.BossLite.Common.Exceptions  import DbError

class DBObjectsTest(unittest.TestCase):
    
    def setUp(self):
        """
        _setUp_
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self.testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)


    def tearDown(self):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return


    def testA_CreateTaskObjects(self):
        """
        Test creation and destruction of task objects.
        """
        
        db = BossLiteDBWM()
        myThread = threading.currentThread()
        
        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)
        
        self.assertFalse(task.exists(db))
        
        task.create(db)
        
        self.assertTrue(task.exists(db))
        
        # Now looks at what's actually there
        queryResult = db.executeSQL(query = "SELECT * FROM bl_task")
        taskInfo = queryResult[0].fetchall()[0].values()
        
        self.assertTrue('Narses' in taskInfo)
        self.assertTrue('Taginae' in taskInfo)

        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.save(db)
        
        queryResult = db.executeSQL(query = "SELECT * FROM bl_task")
        taskInfo = queryResult[0].fetchall()[0].values()
        
        self.assertTrue('Cannae' in taskInfo)
        self.assertTrue('Zama' in taskInfo)

        # Load by ID, test save
        task2 = Task(parameters = {'id': 1})
        task2.load(db)

        self.assertEqual(task2.data['name'], 'Narses')
        self.assertEqual(task2.data['serverName'], 'Taginae')
        self.assertEqual(task2.data['startDirectory'], 'Cannae')
        self.assertEqual(task2.data['outputDirectory'], 'Zama')

        # Load by name
        task3 = Task(parameters = {'name': 'Narses'})
        task3.load(db)

        self.assertEqual(task3.data['name'], 'Narses')
        self.assertEqual(task3.data['serverName'], 'Taginae')
        self.assertEqual(task3.data['startDirectory'], 'Cannae')
        self.assertEqual(task3.data['outputDirectory'], 'Zama')

        task4 = Task(parameters = {'id': 1})
        task4.load(db)
        
        self.assertTrue(task4.exists(db))
        
        task3.remove(db)
        
        self.assertFalse(task4.exists(db))
        
        return


    def testB_CreateJobObjects(self):
        """
        Test creation and destruction of job objects

        """
        
        db = BossLiteDBWM()
        
        task = Task()
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        
        self.assertFalse(job.exists(db))
        
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.save(db)
        
        self.assertTrue(job.exists(db))

        # Can we load by id?  Test our ability to save parameters as well
        # ATTENTION: taskID and jobID must be always present to consistency purposes
        #            'name' is a UUID by default if not specified
        job2 = Job(parameters = {'id': 1, 'jobId': 101, 'taskId': task.exists(db)})
        job2.load(db)
        for key in job2.data.keys():
            self.assertEqual(job2.data[key], job.data[key])
            
        # Test Delete
        self.assertTrue(job2.exists(db))
        
        job2.remove(db)
        
        self.assertFalse(job2.exists(db))
        
        job3 = Job(parameters = {'jobId': 101})
        
        self.assertFalse(job3.exists(db))

        return
    

    def testC_CreateRunningJobs(self):
        """
        Test basic creation, deletion, etc. for RunningJobs

        """
        
        myThread = threading.currentThread()
        db = BossLiteDBWM()
        
        task = Task()
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        job.create(db)
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(db), 'submission': 1})
        
        self.assertFalse(runJob.exists(db))
        
        runJob.create(db)
        
        self.assertTrue(runJob.exists(db))
        
        runJob.data['state'] = 'Commodus'
        runJob.save(db)

        # Test save() and load() by loading file by ID
        runJob2 = RunningJob(parameters = {'id': 1})
        runJob2.load(db)
        
        for key in ['submission', 'jobId', 'taskId', 'state']:
            self.assertEqual(runJob2.data[key], runJob.data[key])

        # Test load by parameters
        runJob3 = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(db), 'submission': 1})
        runJob3.load(db)
        
        for key in ['submission', 'jobId', 'taskId', 'state']:
            self.assertEqual(runJob3.data[key], runJob.data[key])

        # What happens if you load a non-existant job?
        # Note: This test works, but it makes a mess, so I commented it out
        # Uncomment if you want to check it.
        #runJob4 = RunningJob(parameters = {'jobId': 'retarded', 'taskId': task.exists(), 'submission': 1})
        #runJob4.load()

        runJob3.remove(db)
        self.assertFalse(runJob3.exists(db))
        
        return


    def testD_TestAssociations(self):
        """
        Test association between jobs, tasks, etc.

        """
        
        db = BossLiteDBWM()
        
        task = Task()
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        # WFT??? Yes, it is necessary for consistency... orrible!!!
        job.data['submissionNumber'] = 1
        job.create(db)
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(db), 'submission': 1})
        runJob.create(db)
        
        self.assertTrue(job.exists(db))
        self.assertTrue(runJob.exists(db))
        self.assertEqual(job.runningJob, None)
        
        # WFT??? Yes, it is necessary for consistency... orrible!!!
        job.data['submissionNumber'] = runJob['submission']
        job.setRunningInstance(runJob)

        self.assertTrue(job.runningJob != None)
        
        job2 = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        
         # WFT??? Yes, it is necessary for consistency... orrible!!!
        job2.data['submissionNumber'] = runJob.data['submission']
        job2.getRunningInstance(db)   # Load from database
        
        self.assertTrue(job.runningJob != None)
        self.assertEqual(job.runningJob.data['id'], runJob.exists(db))
        
        job.runningJob.data['status'] = 'deceased'
                
        job.updateRunningInstance(db)
        runJob.load(db)
        
        self.assertEqual(runJob.data['status'], 'deceased')
        
        job.closeRunningInstance(db)
        runJob.load(db)
        
        self.assertEqual(runJob.data['closed'], 'Y')

        # Get jobs from task
        self.assertEqual(task.jobs, [])
        
        task.loadJobs(db)
        
        self.assertEqual(len(task.jobs), 1)
        self.assertEqual(task.jobs[0]['jobId'], 101)
        self.assertEqual(task.jobs[0]['taskId'], task.exists(db))

        # Check if task.update works
        # This recursively tests if job.update works
        task.jobs[0].newRunningInstance(db)
        task.jobs[0].runningJob['service'] = 'Unserved'
        
        task.update(db)

        job2 = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        job2.load(db)
        job2.getRunningInstance(db)
        self.assertEqual(job2.runningJob['service'], task.jobs[0].runningJob['service'])
        
        return
    

    def testE_CreateTaskJobsCascade(self):
        """
        Test save task and jobs in cascade on DB

        """
        
        db = BossLiteDBWM()
        nTestJobs = 7
        
        parameters = {'serverName': 'Spartacus', 'name': 'Ludus'}
        task = Task(parameters = parameters)
        
        task.create(db)
        taskId = task.exists(db)
        
        task.data['startDirectory']  = 'Ilithyia'
        task.data['outputDirectory'] = 'Lucretia'
        
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                            'events' : jobId+1000 } )
            task.addJob(job)

        task.save(db)
        self.assertTrue(task.exists(db))
        
        task2 = Task(parameters = {'id': taskId})  
        task2.load(db)
        
        self.assertTrue(task2.exists(db))
        self.assertEqual(task.exists(db), task2.exists(db))
        self.assertEqual(task.data['name'], task2.data['name'])

        self.assertEqual(len(task2.jobs), nTestJobs)
        
        for jobId in range(0, nTestJobs):
            for key in ['name', 'events'] :
                self.assertEqual(task.jobs[jobId].data[key], task2.jobs[jobId].data[key])
          
        return
    

    def testF_DeepUpdate(self):
        """
        Test save task and jobs in cascade on DB

        """
        
        myThread = threading.currentThread()
        db = BossLiteDBWM()
        nTestJobs = 13
        
        parameters = {'serverName': 'Spartacus', 'name': 'Ludus'}
        task = Task(parameters = parameters)
        
        task.create(db)
        taskId = task.exists(db)
        
        task.data['startDirectory']  = 'Ilithyia'
        task.data['outputDirectory'] = 'Lucretia'
        
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                            'events' : jobId+1000 } )
            task.addJob(job)

        task.save(db)
        self.assertTrue(task.exists(db))
        
        self.assertEqual(task.data['serverName'], 'Spartacus')
        task['serverName'] = 'Crixus'
        task.update(db, deep=False)
        self.assertEqual(task.data['serverName'], 'Crixus')
        
        tmp = task.jobs[0].data['events']
        for jobId in range(0, nTestJobs):
            task.jobs[jobId].data['events'] = jobId+2000
            
        self.assertNotEqual(task.jobs[0].data['events'], tmp)
        
        task.update(db, deep=False)

        queryResult = db.executeSQL(query = "SELECT * FROM bl_job WHERE name = 'Doctore-0'")
        jobInfo = queryResult[0].fetchall()[0].values()
    
        self.assertEqual(jobInfo[5], tmp)
        
        task.update(db, deep=True)
        
        queryResult = db.executeSQL(query = "SELECT * FROM bl_job WHERE name = 'Doctore-0'")
        jobInfo = queryResult[0].fetchall()[0].values()
        
        self.assertNotEqual(jobInfo[5], tmp)
        
        return
    
    
    def testG_JobRunningJob(self):
        """
        Test load/save RunningJob correctly

        """ 
        
        db = BossLiteDBWM()
        
        parameters = {'name': 'Bishop'}
        task = Task(parameters)
        parameters = {'name': 'Walter', 'events' : 42 }
        job = Job( parameters )
        job.newRunningInstance(db)
        task.addJob(job)
        
        parameters = {'name': 'Peter', 'events' : 24 }
        job = Job( parameters )
        job.newRunningInstance(db)
        task.addJob(job)
        
        self.assertEqual(task.exists(db), False)
        self.assertEqual(task.existsInDataBase, False)
        
        task.save(db)
        
        self.assertEqual(task.exists(db), 1)
        self.assertEqual(task.existsInDataBase, True)
        
        task2 = Task(parameters = {'id': 1})  
        task2.load(db)
        
        self.assertEqual(task2.existsInDataBase, True)
        self.assertEqual(task.data['name'], task2.data['name'])
        
        loadedJob = task2.getJob(2)
        
        self.assertNotEqual(loadedJob.runningJob, None)
        self.assertEqual(loadedJob['name'], 'Peter')
        self.assertEqual(loadedJob['events'], 24)
        
        loadedJob.runningJob['wrapperReturnCode'] = "60303"
        loadedJob.updateRunningInstance(db)
        loadedJob.closeRunningInstance(db)
        loadedJob.newRunningInstance(db)
        
        self.assertNotEqual(loadedJob.runningJob['wrapperReturnCode'], "60303")
        self.assertNotEqual(loadedJob.runningJob['closed'], "Y")
        
        loadedJob.runningJob['wrapperReturnCode'] = "-1"
        loadedJob.updateRunningInstance(db)
        loadedJob.closeRunningInstance(db)
        
        self.assertEqual(loadedJob.runningJob['closed'], "Y")
        
        parameters = {'applicationReturnCode': '0'}
        run = RunningJob(parameters)
        loadedJob.setRunningInstance(run)
        
        self.assertEqual(loadedJob.runningJob['applicationReturnCode'], "0")
        self.assertNotEqual(loadedJob.runningJob['wrapperReturnCode'], "-1")
        self.assertNotEqual(loadedJob.runningJob['closed'], "Y")
        
        return
    

    def testH_ExceptionHandling(self):
        """
        Test Exception Handling for Task, ...

        """ 
        
        db = BossLiteDBWM()
        
        # invalid Task load, erroneous ID -> exception raised
        task = Task(parameters= {'id': 666})
        try:
            task.load(db)
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (string.find(msg, "task instances corresponds")) != -1 )
        
        # triggering Task removal before save -> exception raised
        task2 = Task(parameters= {'id': 5})
        try:
            task2.remove(db)
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (string.find(msg, "since it is not in the database")) != -1 )
        
        return

    
class DBObjectsPerformance(unittest.TestCase):
    
    def setUp(self):
        """
        _setUp_
        """
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ["WMCore.WMBS"])
        self.testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)


    def tearDown(self):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return
    
    def testA_fullObjects(self):
        """
        Performance test, do not abuse!
        """
        
        numtask = 10
        numjob  = 100
        
        db = BossLiteDBWM()
        log = logging.getLogger( "DBObjectsPerformance" )

        start_time = time.time()

        for t in xrange(numtask):
            try:
                task = Task()
                task.data['name'] = 'task_%s'%str(t)
                task.create(db)
                tmpId = task['id']
                # self.assertEqual(tmpId, task.exists(db))
                
                for j in xrange(numjob):
                    parameters = {'name': '%s_job_%s'%(str(t),str(j)), 
                                  'jobId': j, 
                                  'taskId': tmpId }
                    job = Job(parameters)
                    job.data['submissionNumber'] = 1
                    job.data['closed'] = 'N'
                    
                    parameters = {'jobId': job.data['jobId'], 
                                  'taskId': tmpId,
                                  'submission' : job.data['submissionNumber']}
                    runJob = RunningJob(parameters)
                    runJob.data['state'] = 'Commodus'
                    runJob.data['closed'] = 'N'
                    runJob.data['process_status'] = 'not_handled'
                    
                    job.setRunningInstance(runJob)
                    task.addJob(job)
                task.save(db)
            except DbError, ex:
                # useless...
                print str(ex)
                print "task_" +str(t)
        
        end_time = time.time()
        
        log.info("task= %3d, jobs/task= %3d, runJobs/Job= %3d, Time= %f" % \
            (numtask, numjob, 1, (end_time-start_time)))
        
        return
    
if __name__ == "__main__":
    # Log performance timing...
    LOG_FILENAME = './DBObjects_performance.txt'
    logging.basicConfig(filename=LOG_FILENAME)
    logging.getLogger( "DBObjectsPerformance" ).setLevel( logging.INFO )
    
    # -> suite1: check if objects and relations between objects     
    #            matching all the requirements (the default)
    # -> suite2: run a simple benchmark measuring the time to
    #            save into the db a complete set of objects
    suite1 = unittest.TestLoader().loadTestsFromTestCase(DBObjectsTest)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(DBObjectsPerformance)
    alltests = unittest.TestSuite([suite1, suite2])

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(suite1)
    
