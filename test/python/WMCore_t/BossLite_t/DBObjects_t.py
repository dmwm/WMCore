#!/usr/bin/env python

import unittest
import threading
import string

# Import key features
from WMQuality.TestInit import TestInit


# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

class DBObjectsTest(unittest.TestCase):
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
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
        myThread = threading.currentThread()

        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)
        
        self.assertFalse(task.exists())
        
        task.create()
        
        self.assertTrue(task.exists())

        # Now looks at what's actuallly there
        taskInfo = myThread.dbi.processData("SELECT * FROM bl_task")[0].fetchall()[0].values()

        self.assertTrue('Narses' in taskInfo)
        self.assertTrue('Taginae' in taskInfo)

        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.save()

        #taskInfo = myThread.dbi.processData("SELECT * FROM bl_task")[0].fetchall()[0].values()
        #print taskInfo
        #self.assertTrue('Cannae' in taskInfo)
        #self.assertTrue('Zama' in taskInfo)

        # Load by ID, test save
        task2 = Task(parameters = {'id': 1})
        task2.load()

        self.assertEqual(task2.data['name'], 'Narses')
        self.assertEqual(task2.data['serverName'], 'Taginae')
        self.assertEqual(task2.data['startDirectory'], 'Cannae')
        self.assertEqual(task2.data['outputDirectory'], 'Zama')

        # Load by name
        task3 = Task(parameters = {'name': 'Narses'})
        task3.load()

        self.assertEqual(task3.data['name'], 'Narses')
        self.assertEqual(task3.data['serverName'], 'Taginae')
        self.assertEqual(task3.data['startDirectory'], 'Cannae')
        self.assertEqual(task3.data['outputDirectory'], 'Zama')


        task4 = Task(parameters = {'id': 1})
        task4.load()
        
        self.assertTrue(task4.exists())
        
        task3.remove()
        
        self.assertFalse(task4.exists())
        
        return


    def testB_CreateJobObjects(self):
        """
        Test creation and destruction of job objects

        """
        
        task = Task()
        task.create()
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        
        self.assertFalse(job.exists())
        
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.save()
        
        self.assertTrue(job.exists())

        # Can we load by id?  Test our ability to save parameters as well
        # ATTENTION: taskID and jobID must be always present to consistency purposes
        #            'name' is a UUID by default if not specified
        job2 = Job(parameters = {'id': 1, 'jobId': 101, 'taskId': task.exists()})
        job2.load()
        for key in job2.data.keys():
            self.assertEqual(job2.data[key], job.data[key])
            
        # Test Delete
        self.assertTrue(job2.exists())
        
        job2.remove()
        
        self.assertFalse(job2.exists())
        
        job3 = Job(parameters = {'jobId': 101})
        
        self.assertFalse(job3.exists())

        return
    

    def testC_CreateRunningJobs(self):
        """
        Test basic creation, deletion, etc. for RunningJobs

        """
        myThread = threading.currentThread()

        task = Task()
        task.create()
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        job.create()
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(), 'submission': 1})
        
        self.assertFalse(runJob.exists())
        
        runJob.create()
        
        self.assertTrue(runJob.exists())
        
        runJob.data['state'] = 'Commodus'
        runJob.save()

        # Test save() and load() by loading file by ID
        runJob2 = RunningJob(parameters = {'id': 1})
        runJob2.load()
        
        for key in ['submission', 'jobId', 'taskId', 'state']:
            self.assertEqual(runJob2.data[key], runJob.data[key])

        # Test load by parameters
        runJob3 = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(), 'submission': 1})
        runJob3.load()
        
        for key in ['submission', 'jobId', 'taskId', 'state']:
            self.assertEqual(runJob3.data[key], runJob.data[key])

        # What happens if you load a non-existant job?
        # Note: This test works, but it makes a mess, so I commented it out
        # Uncomment if you want to check it.
        #runJob4 = RunningJob(parameters = {'jobId': 'retarded', 'taskId': task.exists(), 'submission': 1})
        #runJob4.load()

        runJob3.remove()
        self.assertFalse(runJob3.exists())
        
        return


    def testD_TestAssociations(self):
        """
        Test association between jobs, tasks, etc.

        """
        
        task = Task()
        task.create()
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        job.create()
        runJob = RunningJob(parameters = {'jobId': job.data['jobId'], 'taskId': task.exists(), 'submission': 0})
        runJob.create()
        
        self.assertTrue(job.exists())
        self.assertTrue(runJob.exists())

        # Everything should be there
        # Test loading runningJob from job
        self.assertEqual(job.runningJob, None)
        
        job.getRunningInstance()   # Load from database
        
        self.assertTrue(job.runningJob != None)
        self.assertEqual(job.runningJob.data['id'], runJob.exists())
        
        job.runningJob.data['status'] = 'deceased'
        job.updateRunningInstance()
        runJob.load()
        
        self.assertEqual(runJob.data['status'], 'deceased')
        
        job.closeRunningInstance()
        runJob.load()
        
        self.assertEqual(runJob.data['closed'], 'Y')

        # Get jobs from task
        self.assertEqual(task.jobs, [])
        
        task.loadJobs()
        
        self.assertEqual(len(task.jobs), 1)
        self.assertEqual(task.jobs[0]['jobId'], 101)
        self.assertEqual(task.jobs[0]['taskId'], task.exists())

        # Check if task.update works
        # This recursively tests if job.update works
        task.jobs[0].newRunningInstance()
        task.jobs[0].runningJob['service'] = 'Unserved'
        
        task.update()

        job2 = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        job2.load()
        job2.getRunningInstance()
        self.assertEqual(job2.runningJob['service'], task.jobs[0].runningJob['service'])
        
        return
    

    def testE_CreateTaskJobsCascade(self):
        """
        Test save task and jobs in cascade on DB

        """
        
        nTestJobs = 7
        
        parameters = {'serverName': 'Spartacus', 'name': 'Ludus'}
        task = Task(parameters = parameters)
        
        task.create()
        taskId = task.exists()
        
        task.data['startDirectory']  = 'Ilithyia'
        task.data['outputDirectory'] = 'Lucretia'
        
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                            'events' : jobId+1000 } )
            task.addJob(job)

        task.save()
        self.assertTrue(task.exists())
        
        task2 = Task(parameters = {'id': taskId})  
        task2.load()
        
        self.assertTrue(task2.exists())
        self.assertEqual(task.exists(), task2.exists())
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
        nTestJobs = 13
        
        parameters = {'serverName': 'Spartacus', 'name': 'Ludus'}
        task = Task(parameters = parameters)
        
        task.create()
        taskId = task.exists()
        
        task.data['startDirectory']  = 'Ilithyia'
        task.data['outputDirectory'] = 'Lucretia'
        
        for jobId in range(0, nTestJobs):
            job = Job( parameters = {'name': ('Doctore-' + str(jobId)),
                                            'events' : jobId+1000 } )
            task.addJob(job)

        task.save()
        self.assertTrue(task.exists())
        
        self.assertEqual(task.data['serverName'], 'Spartacus')
        task['serverName'] = 'Crixus'
        task.update(deep=False)
        self.assertEqual(task.data['serverName'], 'Crixus')
        
        tmp = task.jobs[0].data['events']
        for jobId in range(0, nTestJobs):
            task.jobs[jobId].data['events'] = jobId+2000
            
        self.assertNotEqual(task.jobs[0].data['events'], tmp)
        
        task.update(deep=False)

        jobInfo = myThread.dbi.processData("SELECT * FROM bl_job WHERE name = 'Doctore-0'")[0].fetchall()[0].values()       
        self.assertEqual(jobInfo[5], tmp)
        
        task.update(deep=True)
        
        jobInfo = myThread.dbi.processData("SELECT * FROM bl_job WHERE name = 'Doctore-0'")[0].fetchall()[0].values()
        self.assertNotEqual(jobInfo[5], tmp)
        
        return
    
    
    def testG_JobRunningJob(self):
        """
        Test load/save RunningJob correctly

        """ 
        
        parameters = {'name': 'Bishop'}
        task = Task(parameters)
        
        parameters = {'name': 'Walter', 'events' : 42 }
        job = Job( parameters )
        job.newRunningInstance(  )
        task.addJob(job)
        
        parameters = {'name': 'Peter', 'events' : 24 }
        job = Job( parameters )
        job.newRunningInstance(  )
        task.addJob(job)
        
        self.assertEqual(task.exists(), False)
        self.assertEqual(task.existsInDataBase, False)
        
        task.save()
        
        self.assertEqual(task.exists(), 1)
        self.assertEqual(task.existsInDataBase, True)
        
        task2 = Task(parameters = {'id': 1})  
        task2.load()
        
        self.assertEqual(task2.existsInDataBase, True)
        self.assertEqual(task.data['name'], task2.data['name'])
        
        loadedJob = task2.getJob(2)
        
        self.assertNotEqual(loadedJob.runningJob, None)
        self.assertEqual(loadedJob['name'], 'Peter')
        self.assertEqual(loadedJob['events'], 24)
        
        loadedJob.runningJob['wrapperReturnCode'] = "60303"
        loadedJob.updateRunningInstance()
        loadedJob.closeRunningInstance()
        loadedJob.newRunningInstance( )
        
        self.assertNotEqual(loadedJob.runningJob['wrapperReturnCode'], "60303")
        self.assertNotEqual(loadedJob.runningJob['closed'], "Y")
        
        loadedJob.runningJob['wrapperReturnCode'] = "-1"
        loadedJob.updateRunningInstance()
        loadedJob.closeRunningInstance()
        
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
        
        # invalid Task load, erroneous ID -> exception raised
        task = Task(parameters= {'id': 666})
        try:
            task.load()
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (string.find(msg, "task instances corresponds")) != -1 )
        
        # triggering Task removal before save -> exception raised
        task2 = Task(parameters= {'id': 5})
        try:
            task2.remove()
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (string.find(msg, "since it is not in the database")) != -1 )
        
        return
    
if __name__ == "__main__":
    unittest.main()
