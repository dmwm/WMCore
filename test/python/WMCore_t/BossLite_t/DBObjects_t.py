#!/usr/bin/env python
"""
_DBObject_t_

"""




import unittest
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
    """
    Unit-test for DbObjects
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
    
    
    def testA_CreateTaskObjects(self):
        """
        Test creation and destruction of task objects.
        """
        
        db = BossLiteDBWM()
        
        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)
        
        self.assertFalse(task.exists(db))
        
        task.create(db)
        
        self.assertTrue(task.exists(db))
        
        queryResult = db.executeSQL(query = """ SELECT * FROM bl_task """)
        taskInfo = queryResult[0].fetchall()[0].values()
        
        self.assertTrue('Narses' in taskInfo)
        self.assertTrue('Taginae' in taskInfo)

        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.save(db)
        
        queryResult = db.executeSQL(query = """ SELECT * FROM bl_task """)
        taskInfo = queryResult[0].fetchall()[0].values()
        
        self.assertTrue('Cannae' in taskInfo)
        self.assertTrue('Zama' in taskInfo)

        task2 = Task(parameters = {'id': 1})
        task2.load(db)

        self.assertEqual(task2.data['name'], 'Narses')
        self.assertEqual(task2.data['serverName'], 'Taginae')
        self.assertEqual(task2.data['startDirectory'], 'Cannae')
        self.assertEqual(task2.data['outputDirectory'], 'Zama')

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
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 
                                'taskId': task.exists(db), 'wmbsJobId': 1 })
        
        self.assertFalse(job.exists(db))
        
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.data['dlsDestination'] = ['file:///www.google.com', 
                                      'http://www.cern.ch',
                                      'C:/windows/explorer.exe' ]
        job.data['wmbsJobId']      = 1
        job.save(db)
        
        self.assertTrue(job.exists(db))
        
        job2 = Job(parameters = {'id': 1, 'jobId': 101, 
                                        'taskId': task.exists(db)})

        job2.load(db)
        for key in job2.data.keys():
            self.assertEqual(job2.data[key], job.data[key])
            
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
        
        db = BossLiteDBWM()
        
        task = Task()
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 
                                'taskId': task.exists(db), 'wmbsJobId': 1 })
        job.create(db)
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 
                                          'taskId': task.exists(db), 
                                          'submission': 1})
        
        self.assertFalse(runJob.exists(db))
        
        runJob.create(db)
        
        self.assertTrue(runJob.exists(db))
        
        runJob.data['state'] = 'Commodus'
        runJob.data['lfn'] = ['001', '002', '003', '004', '005']
        
        # rounding!
        tmpTime = int(time.time())
        runJob.data['startTime'] = None # 0 -> 1970-01-01 00:00:00
        runJob.data['stopTime'] = tmpTime
        runJob.save(db)

        runJob2 = RunningJob(parameters = {'id': 1})
        runJob2.load(db)
        
        for key in ['submission', 'jobId', 'taskId', 'state', 'lfn']:
            self.assertEqual(runJob2.data[key], runJob.data[key])
        
        self.assertEqual(runJob2.data['startTime'], 0 ) # 0 -> None
        self.assertEqual(runJob2.data['stopTime'], tmpTime )
        
        runJob3 = RunningJob(parameters = {'jobId': job.data['jobId'], 
                                           'taskId': task.exists(db), 
                                           'submission': 1} )
        runJob3.load(db)
        
        for key in ['submission', 'jobId', 'taskId', 'state', 'lfn']:
            self.assertEqual(runJob3.data[key], runJob.data[key])
        self.assertEqual(runJob3.data['startTime'], 0 ) 
        self.assertEqual(runJob3.data['stopTime'], tmpTime )
        
        # What happens if you load a non-existant job?
        # Note: This test works, but it makes a mess, so I commented it out
        # Uncomment if you want to check it.
        #runJob4 = RunningJob(parameters = {'jobId': 'retarded', 
        #                                    'taskId': task.exists(), 
        #                                    'submission': 1} )
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
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 
                                'taskId': task.exists(db), 'wmbsJobId': 1 })
        # WFT??? Yes, it is necessary for consistency... orrible!!!
        job.data['submissionNumber'] = 1
        job.create(db)
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 
                                            'taskId': task.exists(db), 
                                            'submission': 1})
        runJob.create(db)
        
        self.assertTrue(job.exists(db))
        self.assertTrue(runJob.exists(db))
        self.assertEqual(job.runningJob, None)
        
        # WFT??? Yes, it is necessary for consistency... orrible!!!
        job.data['submissionNumber'] = runJob['submission']
        job.setRunningInstance(runJob)

        self.assertTrue(job.runningJob != None)
        
        job2 = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 
                                 'taskId': task.exists(db), 'wmbsJobId': 1 })
        
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

        self.assertEqual(task.jobs, [])
        
        task.loadJobs(db)
        
        self.assertEqual(len(task.jobs), 1)
        self.assertEqual(task.jobs[0]['jobId'], 101)
        self.assertEqual(task.jobs[0]['taskId'], task.exists(db))

        task.jobs[0].newRunningInstance(db)
        task.jobs[0].runningJob['service'] = 'Unserved'
        
        task.update(db)
        job2 = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 
                                            'taskId': task.exists(db)})
        job2.load(db)
        
        job2.getRunningInstance(db)
        self.assertEqual(job2.runningJob['service'], 
                                    task.jobs[0].runningJob['service'])
        
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
        
        i = 0
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                     'events' : jobId+1000, 'wmbsJobId': i } )
            i += 1
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
                self.assertEqual(task.jobs[jobId].data[key], 
                                            task2.jobs[jobId].data[key])
          
        return
    
    
    def testF_DeepUpdate(self):
        """
        Test save task and jobs in cascade on DB
        """
        
        db = BossLiteDBWM()
        nTestJobs = 13
        
        parameters = {'serverName': 'Spartacus', 'name': 'Ludus'}
        task = Task(parameters = parameters)
        
        task.create(db)
        taskId = task.exists(db)
        
        task.data['startDirectory']  = 'Ilithyia'
        task.data['outputDirectory'] = 'Lucretia'

        i = 0
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                     'events' : jobId+1000, 'wmbsJobId': i } )
            i += 1
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

        queryResult = db.executeSQL(query = """ SELECT * FROM bl_job
                                                WHERE name = 'Doctore-0' """)
        jobInfo = queryResult[0].fetchall()[0].values()
        
        self.assertEqual(jobInfo[6], tmp)
        
        task.update(db, deep=True)
        
        queryResult = db.executeSQL(query = """ SELECT * FROM bl_job
                                                WHERE name = 'Doctore-0' """)
        jobInfo = queryResult[0].fetchall()[0].values()
        
        self.assertNotEqual(jobInfo[6], tmp)
        
        return
    
    
    def testG_JobRunningJob(self):
        """
        Test load/save RunningJob correctly
        """ 
        
        db = BossLiteDBWM()
        
        parameters = {'name': 'Bishop'}
        task = Task(parameters)
        parameters = {'name': 'Walter', 'events' : 42, 'wmbsJobId': 1 }
        job = Job( parameters )
        job.newRunningInstance(db)
        task.addJob(job)
        
        parameters = {'name': 'Peter', 'events' : 24, 'wmbsJobId': 2 }
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
            self.assertTrue( (msg.find("task instances corresponds")) != -1 )
        
        # triggering Task removal before save -> exception raised
        task2 = Task(parameters= {'id': 5})
        try:
            task2.remove(db)
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (msg.find("is not in the database")) != -1 )
        
        return

    
class DBObjectsPerformance(unittest.TestCase):
    """
    Simple unit-test to measure DB performances
    """
    
    numtask = 1
    numjob  = 10
    
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
        
        self.testInit.attemptToCloseDBConnections()
        self.testInit.clearDatabase()
        
        return
    
    
    def testA_createAndSaveObjects(self):
        """
        Performance test, do not abuse!
        """
        
        db = BossLiteDBWM()
        log = logging.getLogger( "DBObjectsPerformance" )

        start_time = time.time()

        for t in xrange(self.numtask):
            try:
                task = Task()
                task.data['name'] = 'task_%s'% str(t)
                task.create(db)
                tmpId = task['id']
                
                self.assertEqual(tmpId, task.exists(db))
                
                task.exists(db)
                i = 0
                for j in xrange(self.numjob):
                    parameters = {'name': '%s_job_%s' % (str(t), str(j)), 
                                  'jobId': j, 
                                  'taskId': tmpId,
                                  'wmbsJobId': i }
                    i += 1
                    job = Job(parameters)
                    job.data['closed'] = 'N'
                                        
                    runJob = RunningJob()
                    runJob.data['state'] = 'Commodus'
                    runJob.data['closed'] = 'N'
                    runJob.data['process_status'] = 'not_handled'
                    
                    job.newRunningInstance(db)
                    
                    task.addJob(job)
                    
                task.save(db)
                
            except DbError, ex:
                # useless...
                print str(ex)
                print "task_" +str(t)
        
        end_time = time.time()
        
        log.info("SAVE: task= %3d, jobs/task= %3d, Time= %f" % \
            (self.numtask, self.numjob, (end_time-start_time)))
        
        return
    
    
    def testB_LoadObjects(self):
        """
        testC_databaseIsPersistent
        """
        
        db = BossLiteDBWM()
        log = logging.getLogger( "DBObjectsPerformance" )
        self.testA_createAndSaveObjects()

        task = Task(parameters = {'id': 1})
                
        start_time = time.time()
        
        task.load(db)
        
        end_time = time.time()
        
        log.info("LOAD: Time= %f" % ( (end_time-start_time) ) )
        
        self.assertEqual(1, task.exists(db))
        self.assertEqual(self.numjob, len(task.jobs))
        
        return
    
    
    
if __name__ == "__main__":
    # Log performance timing...
    LOG_FILENAME = './DBObjects_performance.txt'
    logging.basicConfig(filename=LOG_FILENAME)
    logging.getLogger( "DBObjectsPerformance" ).setLevel( logging.INFO )
    
    # -> unitA: check if objects and relations between objects     
    #            matching all the requirements (the default)
    # -> unitB: run a simple benchmark measuring the time to
    #            save into the db a complete set of objects
    unitA = unittest.TestLoader().loadTestsFromTestCase(DBObjectsTest)
    unitB = unittest.TestLoader().loadTestsFromTestCase(DBObjectsPerformance)
    unitComplete = unittest.TestSuite([unitA, unitB])

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(unitA)
    unittest.TextTestRunner(verbosity=3).run(unitB)
    
