#!/usr/bin/env python

__revision__ = "$Id: API_t.py,v 1.7 2010/05/17 13:07:43 spigafi Exp $"
__version__ = "$Revision: 1.7 $"

import unittest
import threading
import string

# Import key features
from WMQuality.TestInit import TestInit

# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

# Import API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI

# database engine
from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM

def populateDb(db, numtask, numjob):
    """
    This procedure populates the DB. It is time expensive and 
    it could be improved (for example loading a pre-existing database)
    """

    for t in xrange(numtask):
        task = Task()
        task.data['name'] = 'task_%s'%str(t)
        task.create(db)
        tmpId = task['id']
        # self.assertEqual(tmpId, task.exists(db))
        task.exists(db)
        for j in xrange(numjob):
            parameters = {'name': '%s_job_%s'%(str(t),str(j)), 
                          'jobId': j, 
                          'taskId': tmpId }
            job = Job(parameters)
            job.data['closed'] = 'N'
            
            # job.save(db, deep= False)
            
            runJob = RunningJob()
            runJob.data['state'] = 'Commodus'
            runJob.data['closed'] = 'N'
            runJob.data['process_status'] = 'not_handled'
            
            job.newRunningInstance(db)
            
            task.addJob(job)
            
        task.save(db)
    
    return task

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

    def testA_APITaskSerializeDeserialize(self):
        """
        test BossLiteAPI.serialize & BossLiteAPI.deserialize
        """

        import os
        import tempfile

        testAPI = BossLiteAPI()
        db = BossLiteDBWM()

        task = populateDb(db, numtask= 1, numjob= 5)
        
        # this produces an XML (str)
        encodedTask = testAPI.serialize(task)
        
        # deserialize reads a file -> I save the file before call the method
        tmp, fname = tempfile.mkstemp( suffix = '.xml', prefix = 'API_t_',
                                       dir = os.getcwd() )
        tmpFile = os.fdopen(tmp, "w")
        tmpFile.write( encodedTask )
        tmpFile.close()
        
        # this reads an XML (file)
        decodedTask = testAPI.deserialize(fname)
        
        # it works but I don't understand the 'decodedTask' format, however...
        # check Task
        self.assertEqual(task['name'], decodedTask[0]['name'])
        
        #check Job
        tmp = task.getJob(2)
        self.assertEqual(tmp['jobId'], int(decodedTask[1][1]['jobId']))

        # check RunningJob
        self.assertEqual(tmp.runningJob['submission'], \
                         int(decodedTask[2][tmp['name']]['submission']))
        
        # this method doesn't work at the moment 
        #newTask= testAPI.declare(fname)
        #self.assertFalse(newTask.exists(db)
                         
        return

    def testB_APITaskMethods(self):

        testAPI = BossLiteAPI()
        db = BossLiteDBWM()

        # Start with task
        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)

        # Can we create it?
        self.assertFalse(task.exists(db))
        
        testAPI.saveTask(task = task)
        
        self.assertTrue(task.exists(db))

        # Can we save and load it?
        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.data['user_proxy']      = 'Barca'
        testAPI.saveTask(task = task)
        task2 = testAPI.loadTask(taskId = task.exists(db))

        for key in task.data.keys():
            self.assertEqual(task.data[key], task2.data[key])

        task3 = testAPI.loadTaskByName(name = 'Narses')

        for key in task.data.keys():
            self.assertEqual(task.data[key], task3.data[key])


        # 'loadTasksByAttr' returns a list
        listofTasks = testAPI.loadTasksByAttr(binds = {'user_proxy' : 'Barca'})
        
        for x in listofTasks :
            for key in task.data.keys():
                self.assertEqual(task.data[key], x.data[key])

        taskA1 = Task(parameters = {'name': 'Octavian',
                                    'user_proxy': 'MarkAntony',
                                    'startDirectory': 'Actium'})
        taskA2 = Task(parameters = {'name': 'Augustus',
                                    'user_proxy': 'MarkAntony',
                                    'startDirectory': 'Actium'})
        taskA1.save(db)
        taskA2.save(db)
        taskA1.exists(db)  # Load the IDs
        taskA2.exists(db)
        result = testAPI.loadTasksByAttr(binds = {'user_proxy' : 'MarkAntony'})
        
        self.assertEqual(len(result), 2)
        for res in result:
            # It's either one or the other
            self.assertTrue(res.data == taskA1.data or res.data == taskA2.data)

        testAPI.removeTask(task = taskA1)
        
        self.assertFalse(taskA1.exists(db))

        return


    def testC_APIJobMethods(self):

        testAPI = BossLiteAPI()
        db = BossLiteDBWM()

        # First create a job
        task = Task()
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        self.assertFalse(job.exists(db))
        
        job.create(db)
        
        self.assertTrue(job.exists(db))
        
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.save(db)

        # Can we load by jobID?
        job2 = testAPI.loadJob(taskId = task.exists(db), jobId = 101)
        
        for key in job.data.keys():
            self.assertEqual(job2.data[key], job.data[key])

        # this test is not interesting because we usually load a job AFTER 
        # the source task is loaded. Call "testAPI.loadJobByName" breaks 
        # consistency checks over "taskId,jobId,name" 
        try:
            job3 = testAPI.loadJobByName(jobName = 'Hadrian')
        except Exception, ex:
            msg = str(ex)
            self.assertTrue( (string.find(msg, "it is not completely specified")) != -1 )
        
        # "loadJobsByAttr" calls directly "Job.SelectJob" DAO... is this the
        # right approach? Cross-check with original implementation...
        jobs = testAPI.loadJobsByAttr( binds = {'name' : 'Hadrian'})
        
        self.assertEqual(len(jobs), 1)
        
        for key in job.data.keys():
            self.assertEqual(jobs[0].data[key], job.data[key])
        

        task2 = testAPI.getTaskFromJob(job = jobs[0])

        testAPI.removeJob(job = jobs[0])
        
        self.assertFalse(jobs[0].exists(db))

        return
    
    
"""
    def testD_APIRunningJobMethods(self):

        testAPI = BossLiteAPI()
        db = BossLiteDBWM()
        
        # First create a job
        task = Task(db)
        task.create(db)
        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        job.create(db)
        
        self.assertEqual(job.runningJob, None)

        testAPI.getNewRunningInstance(job = job, runningAttrs = {'status': 'Dead',
                                                                 'statusReason': 'WentToTheForum'} )

        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')

        job.runningJob.save(db)

        # Load the job
        task.loadJobs(db)

        job.runningJob['service'] = 'IdesOfMarch'
        job.updateRunningInstance(db)

        # Try this from loading the job
        # TEMPORARY DISABLED, see "testB_APIJobMethods" comments...
        #
        #job2['submissionNumber'] = 1
        #job2 = testAPI.loadJobByName(jobName = 'Hadrian')
        #
        #testAPI.getRunningInstance(job = job2, runningAttrs = {'storage': 'Ravenna'})
        #
        #self.assertEqual(job2.runningJob['jobId'], 101)
        #self.assertEqual(job2.runningJob['submission'], 1)
        #self.assertEqual(job2.runningJob['status'], 'Dead')
        #self.assertEqual(job2.runningJob['statusReason'], 'WentToTheForum')
        #self.assertEqual(job2.runningJob['storage'], None)
        #self.assertEqual(job2.runningJob['service'], 'IdesOfMarch')
        

        # Now see if we get a new one with a new job
        job3 = Job(parameters = {'name': 'Trajan', 'jobId': 102, 'taskId': task.exists(db)})
        job3.create(db)
        testAPI.getRunningInstance(job = job3, runningAttrs = {'storage': 'Ravenna'})
        
        self.assertEqual(job3.runningJob['jobId'], 102)
        self.assertEqual(job3.runningJob['submission'], 1)
        self.assertEqual(job3.runningJob['status'], None)
        self.assertEqual(job3.runningJob['statusReason'], None)
        self.assertEqual(job3.runningJob['storage'], 'Ravenna')

        # Now test if we can load by attribute
        # "loadJobsByRunningAttr" calls directly "Job.LoadByRunningJobAttr" DAO... is this the
        # right approach? Cross-check with original implementation...
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
"""
    
"""
    def testE_APICombinedMethods(self):


        testAPI = BossLiteAPI()
        db = BossLiteDBWM()

        # First create a job
        task = Task(db)
        task.create(db)

        job = Job(parameters = {'name': 'Hadrian', 'jobId': 101, 'taskId': task.exists(db)})
        job.create(db)
        
        self.assertEqual(job.runningJob, None)

        testAPI.getNewRunningInstance(job = job, runningAttrs = {'status': 'Dead',
                                                                 'statusReason': 'WentToTheForum'} )

        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')

        job.runningJob.save(db)
        job.save(db)
        task2 = Task(db)
        task2 = testAPI.load(task = task, jobRange = "all")
        
        for key in task.data.keys():
            self.assertEqual(task2.data[key], task.data[key])
        for jobIter in task.jobs:
            for key in jobIter.data.keys():
                self.assertEqual(jobIter.data[key], job[key])
            for key in ['jobId', 'submission', 'status', 'statusReason']:
                self.assertEqual(jobIter.runningJob.data[key], job.runningJob.data[key])
        
        return
"""

if __name__ == "__main__":
    unittest.main()
