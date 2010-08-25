#!/usr/bin/env python

__revision__ = "$Id: API_t.py,v 1.2 2010/04/15 20:51:55 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import threading

# Import key features
from WMQuality.TestInit import TestInit


# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

# Import API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI

class APITest(unittest.TestCase):
    """
    Unittest for the BossLiteAPI

    """
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.BossLite"],
                                useDefault = False)


    def tearDown(self):
        """
        Tear down database

        """

        self.testInit.clearDatabase()

        return


    def testA_APITaskMethods(self):
        """
        See if you can load and run some basic calls from the API

        """

        testAPI = BossLiteAPI()


        # Start with task
        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)

        # Can we create it?
        self.assertFalse(task.exists())
        testAPI.saveTask(task = task)
        self.assertTrue(task.exists())

        # Can we save and load it?
        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.data['user_proxy']      = 'Barca'
        testAPI.saveTask(task = task)

        task2 = testAPI.loadTask(taskId = task.exists())
        for key in task.data.keys():
            self.assertEqual(task.data[key], task2.data[key])


        task3 = testAPI.loadTaskByName(name = 'Narses')
        for key in task.data.keys():
            self.assertEqual(task.data[key], task3.data[key])

        task4 = testAPI.loadTasksByProxy(name = 'Barca')[0]
        for key in task.data.keys():
            self.assertEqual(task.data[key], task4.data[key])


        taskA1 = Task(parameters = {'name': 'Octavian',
                                    'user_proxy': 'MarkAntony',
                                    'startDirectory': 'Actium'})
        taskA2 = Task(parameters = {'name': 'Augustus',
                                    'user_proxy': 'MarkAntony',
                                    'startDirectory': 'Actium'})
        taskA1.save()
        taskA2.save()
        taskA1.exists()  # Load the IDs
        taskA2.exists()
        result = testAPI.loadTasksByProxy(name = 'MarkAntony')
        self.assertEqual(len(result), 2)
        for res in result:
            # It's either one or the other
            self.assertTrue(res.data == taskA1.data or res.data == taskA2.data)


        return


    def testB_APIJobMethods(self):
        """
        Basic methods for handling jobs
        
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create()

        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        self.assertFalse(job.exists())
        job.create()
        self.assertTrue(job.exists())
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.save()

        # Can we load by jobID?
        job2 = testAPI.loadJob(taskId = task.exists(), jobId = 101)
        for key in job.data.keys():
            self.assertEqual(job2.data[key], job.data[key])

        job3 = testAPI.loadJobByName(jobName = 'Hadrian')
        for key in job.data.keys():
            self.assertEqual(job3.data[key], job.data[key])

        job4 = testAPI.loadJobsByAttr( jobAttribute = 'name', value = 'Hadrian')[0]
        for key in job.data.keys():
            self.assertEqual(job4.data[key], job.data[key])


        task2 = testAPI.getTaskFromJob(job = job4)


    def testC_APIRunningJobMethods(self):
        """
        Basic methods for RunningJobs

        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create()

        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        job.create()
        self.assertEqual(job.runningJob, None)

        testAPI.getNewRunningInstance(job = job, runningAttrs = {'status': 'Dead',
                                                                 'statusReason': 'WentToTheForum'} )

        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')

        job.runningJob.save()

        # Load the job
        task.loadJobs()

        job.runningJob['service'] = 'IdesOfMarch'
        job.updateRunningInstance()


        # Try this from loading the job
        job2 = testAPI.loadJobByName(jobName = 'Hadrian')
        job2['submissionNumber'] = 1

        testAPI.getRunningInstance(job = job2, runningAttrs = {'storage': 'Ravenna'})

        self.assertEqual(job2.runningJob['jobId'], 101)
        self.assertEqual(job2.runningJob['submission'], 1)
        self.assertEqual(job2.runningJob['status'], 'Dead')
        self.assertEqual(job2.runningJob['statusReason'], 'WentToTheForum')
        self.assertEqual(job2.runningJob['storage'], None)
        self.assertEqual(job2.runningJob['service'], 'IdesOfMarch')


        # Now see if we get a new one with a new job
        job3 = Job(parameters = {'name': 'Trajan', 'jobId': 102, 'taskId': task.exists()})
        job3.create()

        testAPI.getRunningInstance(job = job3, runningAttrs = {'storage': 'Ravenna'})
        self.assertEqual(job3.runningJob['jobId'], 102)
        self.assertEqual(job3.runningJob['submission'], 1)
        self.assertEqual(job3.runningJob['status'], None)
        self.assertEqual(job3.runningJob['statusReason'], None)
        self.assertEqual(job3.runningJob['storage'], 'Ravenna')

        # Now test if we can load by attribute
        jobList = testAPI.loadJobsByRunningAttr(attribute = 'status', value = 'Dead')
        self.assertEqual(len(jobList), 1)
        job4 = jobList[0]
        job4['submissionNumber'] = 1
        testAPI.getRunningInstance(job = job4)
        self.assertEqual(job4.runningJob['jobId'], 101)
        self.assertEqual(job4.runningJob['submission'], 1)
        self.assertEqual(job4.runningJob['status'], 'Dead')
        self.assertEqual(job4.runningJob['statusReason'], 'WentToTheForum')
        self.assertEqual(job4.runningJob['storage'], None)
        self.assertEqual(job4.runningJob['service'], 'IdesOfMarch')

        return


    def testD_APICombinedMethods(self):
        """

        Test those methods that depend on the API caling multiple data types.

        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create()

        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists()})
        job.create()
        self.assertEqual(job.runningJob, None)

        testAPI.getNewRunningInstance(job = job, runningAttrs = {'status': 'Dead',
                                                                 'statusReason': 'WentToTheForum'} )

        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')

        job.runningJob.save()
        job.save()


        task2 = Task()
        task2 = testAPI.load(task = task, jobRange = "all")
        for key in task.data.keys():
            self.assertEqual(task2.data[key], task.data[key])
        for jobIter in task.jobs:
            for key in jobIter.data.keys():
                self.assertEqual(jobIter.data[key], job[key])
            for key in ['jobId', 'submission', 'status', 'statusReason']:
                self.assertEqual(jobIter.runningJob.data[key], job.runningJob.data[key])
        

        


if __name__ == "__main__":
    unittest.main()
