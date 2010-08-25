#!/usr/bin/env python
"""
_API_t_

"""

__revision__ = "$Id: API_t.py,v 1.11 2010/05/19 10:01:37 spigafi Exp $"
__version__ = "$Revision: 1.11 $"

import unittest
import os
import tempfile
        
# Import key features
from WMQuality.TestInit import TestInit

# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

# Import API
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI


def populateDb(db, numtask, numjob):
    """
    This procedure populates the DB. It is time expensive and 
    it could be improved (for example loading a pre-existing database)
    """

    for t in xrange(numtask):
        task = Task()
        task.data['name'] = 'task_%s' % str(t)
        task.create(db)
        tmpId = task['id']
        # self.assertEqual(tmpId, task.exists(db))
        task.exists(db)
        for j in xrange(numjob):
            parameters = {'name': '%s_job_%s' % (str(t), str(j)), 
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
    Unit-test for BossLiteAPI
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

    
    def testA_TaskSerializeDeserialize(self):
        """
        test BossLiteAPI.serialize & BossLiteAPI.deserialize
        """
        
        testAPI = BossLiteAPI()

        task = populateDb(testAPI.db, numtask= 1, numjob= 5)
        
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
        
        # remove tmp XML file
        os.unlink( fname )
        
        return
    
    
    def testB_TaskMethods(self):
        """
        put a description here
        """

        testAPI = BossLiteAPI()

        # Start with task
        parameters = {'serverName': 'Taginae', 'name': 'Narses'}
        task = Task(parameters = parameters)

        # Can we create it?
        self.assertFalse(task.exists(testAPI.db))
        
        testAPI.saveTask(task = task)
        
        self.assertTrue(task.exists(testAPI.db))

        # Can we save and load it?
        task.data['startDirectory']  = 'Cannae'
        task.data['outputDirectory'] = 'Zama'
        task.data['user_proxy']      = 'Barca'
        testAPI.saveTask(task = task)
        task2 = testAPI.loadTask(taskId = task.exists(testAPI.db))

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
        taskA1.save(testAPI.db)
        taskA2.save(testAPI.db)
        taskA1.exists(testAPI.db)  # Load the IDs
        taskA2.exists(testAPI.db)
        result = testAPI.loadTasksByAttr(binds = {'user_proxy' : 'MarkAntony'})
        
        self.assertEqual(len(result), 2)
        for res in result:
            # It's either one or the other
            self.assertTrue(res.data == taskA1.data or res.data == taskA2.data)

        testAPI.removeTask(task = taskA1)
        
        self.assertFalse(taskA1.exists(testAPI.db))

        return


    def testC_JobMethods(self):
        """
        put a description here
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create(testAPI.db)
        tmp = task.exists(testAPI.db)
        
        self.assertTrue(tmp)
        
        job = Job(parameters = {'name': 'Hadrian', 
                                'jobId': 101, 
                                'taskId': tmp})
        self.assertFalse(job.exists(testAPI.db))
        
        job.create(testAPI.db)
        
        self.assertTrue(job.exists(testAPI.db))
        
        job.data['standardOutput'] = 'MarcusAurelius'
        job.data['standardError']  = 'AntoniousPius'
        job.save(testAPI.db)

        # Can we load by jobID?
        job2 = testAPI.loadJob(taskId = task.exists(testAPI.db), jobId = 101)
        
        for key in job.data.keys():
            self.assertEqual(job2.data[key], job.data[key])

        job3 = testAPI.loadJobByName(jobName = 'Hadrian')
        self.assertEqual(job3.data['jobId'], 101)
        self.assertEqual(job3.data['taskId'], tmp)
        
        jobs = testAPI.loadJobsByAttr( binds = {'name' : 'Hadrian'})
        
        self.assertEqual(len(jobs), 1)
        
        for key in job.data.keys():
            self.assertEqual(jobs[0].data[key], job.data[key])
        

        task2 = testAPI.getTaskFromJob(job = jobs[0])
        
        self.assertEqual(task2.data['id'], tmp)
        
        testAPI.removeJob(job = jobs[0])
        
        self.assertFalse(jobs[0].exists(testAPI.db))

        return
    
    
    def testD_RunningJobMethods(self):
        """
        put a description here
        """

        testAPI = BossLiteAPI()
        
        # First create a job
        task = Task()
        task.create(testAPI.db)
        job = Job(parameters = {'name': 'Hadrian', 
                                'jobId': 101, 
                                'taskId': task.exists(testAPI.db)})
        job.create(testAPI.db)
        
        self.assertEqual(job.runningJob, None)

        testAPI.getNewRunningInstance(job = job, 
                    runningAttrs = {'status': 'Dead', 
                                    'statusReason': 'WentToTheForum'} )

        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')

        job.runningJob.save(testAPI.db)

        # Load the job
        task.loadJobs(testAPI.db)

        job.runningJob['service'] = 'IdesOfMarch'
        job.updateRunningInstance(testAPI.db)
          
        job3 = Job(parameters = {'name': 'Trajan', 
                                 'jobId': 102, 
                                 'taskId': task.exists(testAPI.db)})
        job3.create(testAPI.db)
        
        testAPI.getRunningInstance(job = job3, 
                                   runningAttrs = {'storage': 'Ravenna'})
        
        self.assertEqual(job3.runningJob['jobId'], 102)
        self.assertEqual(job3.runningJob['submission'], 1)
        self.assertEqual(job3.runningJob['status'], None)
        self.assertEqual(job3.runningJob['statusReason'], None)
        self.assertEqual(job3.runningJob['storage'], 'Ravenna')

        # Now test if we can load by attribute
        jobList = testAPI.loadJobsByRunningAttr( attribute = 'status', 
                                                 value = 'Dead' )
        
        self.assertEqual(len(jobList), 1)
        
        job4 = jobList[0]
        job4['submissionNumber'] = 1 # need to ckeck...
        testAPI.getRunningInstance(job = job4)
        
        self.assertEqual(job4.runningJob['jobId'], 101)
        self.assertEqual(job4.runningJob['submission'], 1)
        self.assertEqual(job4.runningJob['status'], 'Dead')
        self.assertEqual(job4.runningJob['statusReason'], 'WentToTheForum')
        
        return
    
    
    def testE_CombinedMethods(self):
        """
        This is only another test ...
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create(testAPI.db)
        job = Job(parameters = {'name': 'Hadrian', 
                                'jobId': 101, 
                                'taskId': task.exists(testAPI.db)})
        job.create(testAPI.db)
        
        self.assertEqual(job.runningJob, None)
        
        testAPI.getNewRunningInstance(job = job, 
                    runningAttrs = {'status': 'Dead', 
                                    'statusReason': 'WentToTheForum'} )
        
        self.assertEqual(job.runningJob['jobId'], 101)
        self.assertEqual(job.runningJob['submission'], 1)
        self.assertEqual(job.runningJob['status'], 'Dead')
        self.assertEqual(job.runningJob['statusReason'], 'WentToTheForum')
        
        job.runningJob.save(testAPI.db)
        job.save(testAPI.db)
        task2 = Task()
        
        task2 = testAPI.loadTask(taskId = task.exists(testAPI.db), 
                                                        jobRange = "all")
        
        for key in task.data.keys():
            self.assertEqual(task2.data[key], task.data[key])
        for jobIter in task.jobs:
            for key in jobIter.data.keys():
                self.assertEqual(jobIter.data[key], job[key])
            for key in ['jobId', 'submission', 'status', 'statusReason']:
                self.assertEqual(jobIter.runningJob.data[key], 
                                                  job.runningJob.data[key])
        
        return
    
    
    def testF_ByRunningAttr(self):
        """
        This is only another test ...
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.save(testAPI.db)
        
        tmp = task.exists(testAPI.db)
        
        # 'jobId' MUSt starts from 1. 
        # Take a look at Task.addJob --> it always overrides 'jobId'!
        job = Job(parameters = {'name': 'Spartacus', 
                                'taskId': tmp, 
                                'jobId': 1} )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'W'} )
        self.assertNotEqual(job.runningJob, None)  
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Fringe', 
                                'taskId': tmp, 
                                'jobId': 2 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'SD'} )
        self.assertNotEqual(job.runningJob, None)  
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Stargate Universe', 
                                'taskId': tmp, 
                                'jobId': 3 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'A'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Caprica', 
                                'taskId': tmp, 
                                'jobId': 4 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'K'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'The Mentalist', 
                                'taskId': tmp, 
                                'jobId': 5 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                      runningAttrs = {'closed' : 'N'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        task.save(testAPI.db)
         
        result = testAPI.loadCreated()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Spartacus')  
        
        result = testAPI.loadSubmitted()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'The Mentalist') 
        
        result = testAPI.loadEnded()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Fringe') 
        
        result = testAPI.loadFailed()
        self.assertEqual(len(result), 2)
        
        self.assertEqual(result[0]['name'], 'Stargate Universe') 
        self.assertEqual(result[1]['name'], 'Caprica') 
        
        return
    
    
if __name__ == "__main__":
    APIsuite = unittest.TestLoader().loadTestsFromTestCase(APITest)

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(APIsuite)
