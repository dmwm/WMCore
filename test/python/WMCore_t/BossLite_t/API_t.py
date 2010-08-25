#!/usr/bin/env python
"""
_API_t_

"""




import unittest
import os
import tempfile
import time
        
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
        i = 0
        for j in xrange(numjob):
            parameters = {'name': '%s_job_%s' % (str(t), str(j)), 
                          'jobId': j, 
                          'taskId': tmpId }
            job = Job(parameters)
            job.data['closed'] = 'N'
            job.data['wmbsJobId'] = i
            i += 1
            
            # job.save(db, deep= False)
            
            runJob = RunningJob()
            runJob.data['state'] = 'Commodus'
            runJob.data['closed'] = 'N'
            runJob.data['processStatus'] = 'not_handled'
            
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
        Test BossLiteAPI.serialize & BossLiteAPI.deserialize
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
        Test Task methods
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
        Test Job methods
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create(testAPI.db)
        tmp = task.exists(testAPI.db)
        
        self.assertTrue(tmp)
        
        job = Job(parameters = {'name': 'Hadrian', 
                                'jobId': 101, 
                                'taskId': tmp,
                                'wmbsJobId': 1})
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
        Test RunningJob methods
        """

        testAPI = BossLiteAPI()
        
        task = Task()
        testAPI.saveTask(task)
                
        job1 = Job(parameters = { 'standardError' : 'hostname.err',
                                  'standardOutput' : 'hostname.out',
                                  'wmbsJobId': 1 } )
        task.addJob(job1)
        
        testAPI.updateDB(job1)
        tmp = testAPI.getNewRunningInstance(job = task.jobs[0], runningAttrs = { 
                                            'processStatus' :  'not_handled',
                                            'service' : 'vattelapesca',
                                            'status' : 'W' } )
        
        self.assertEqual(task.jobs[0].runningJob['status'], 'W')
        self.assertEqual(task.jobs[0].runningJob['submission'], 1)
        self.assertEqual(task.jobs[0].runningJob['submission'], 
                                    job1['submissionNumber'])
        
        testAPI.saveTask(task)
        #testAPI.updateDB(task)
        
        job2 = Job(parameters = { 'standardError' : 'date.err',
                                  'standardOutput' : 'date.out',
                                  'wmbsJobId': 2 } )
        task.addJob(job2)
        testAPI.updateDB(job2)
        
        tmp = testAPI.getNewRunningInstance(job = task.jobs[1], runningAttrs = { 
                                            'processStatus' :  'handled',
                                            'service' : 'cippirimerlo',
                                            'status' : 'F' } )
        
        self.assertEqual(task.jobs[1].runningJob['status'], 'F')
        
        tmp = testAPI.getNewRunningInstance(job = task.jobs[1], runningAttrs = { 
                                            'processStatus' :  'not_handled',
                                            'service' : 'cippirimerlo',
                                            'status' : 'D' } )
        
        self.assertEqual(task.jobs[1].runningJob['status'], 'D')
        
        tmp = testAPI.getNewRunningInstance(job = task.jobs[1], runningAttrs = { 
                                            'processStatus' :  'not_handled',
                                            'service' : 'cippirimerlo',
                                            'status' : 'T' } )
        
        self.assertEqual(task.jobs[1].runningJob['status'], 'T') 
        
        self.assertEqual(task.jobs[1].runningJob['status'], 'T')
        self.assertEqual(task.jobs[1].runningJob['submission'], 3)
        self.assertEqual(task.jobs[1].runningJob['submission'], 
                                    job2['submissionNumber'])
        
        testAPI.saveTask(task)
        #testAPI.updateDB(task)
        
        job3 = Job(parameters = { 'standardError' : 'top.err',
                                  'standardOutput' : 'top.out',
                                  'wmbsJobId': 3 } )
        task.addJob(job3)
        testAPI.updateDB(job3)
        tmp = testAPI.getNewRunningInstance(job = task.jobs[2], runningAttrs = { 
                                            'processStatus' :  'in_progress',
                                            'service' : 'cacao',
                                            'status' : 'T' } ) 
        
        self.assertEqual(task.jobs[2].runningJob['status'], 'T')
        self.assertEqual(task.jobs[2].runningJob['submission'], 1)
        self.assertEqual(task.jobs[2].runningJob['submission'], 
                                    job3['submissionNumber'])
          
        testAPI.saveTask(task)
        #testAPI.updateDB(task)
        
        job4 = Job(parameters = { 'standardError' : 'uname.err',
                                  'standardOutput' : 'uname.out',
                                  'wmbsJobId': 4 } )
        task.addJob(job4)
        testAPI.updateDB(job4)
        
        tmp = testAPI.getNewRunningInstance(job = task.jobs[3], runningAttrs = { 
                                            'processStatus' :  'handled',
                                            'service' : 'ramato',
                                            'status' : 'F' } )
        
        tmp = testAPI.getNewRunningInstance(job = task.jobs[3], runningAttrs = { 
                                            'processStatus' :  'not_handled',
                                            'service' : 'ramato',
                                            'status' : 'S' } )   
        
        self.assertEqual(task.jobs[3].runningJob['status'], 'S')
        self.assertEqual(task.jobs[3].runningJob['submission'], 2)
        self.assertEqual(task.jobs[3].runningJob['submission'], 
                                    job4['submissionNumber'])
        
        testAPI.saveTask(task)
        #testAPI.updateDB(task)
        
        loadedTask = testAPI.loadTask(taskId = task['id'])
        
        self.assertEqual(len(loadedTask.jobs), len(task.jobs))
        self.assertEqual(loadedTask.jobs[0].runningJob['status'], 'W')
        self.assertEqual(loadedTask.jobs[1].runningJob['status'], 'T')
        self.assertEqual(loadedTask.jobs[2].runningJob['status'], 'T')
        self.assertEqual(loadedTask.jobs[3].runningJob['status'], 'S')
        
        self.assertEqual(loadedTask.jobs[0].runningJob['processStatus'], 
                                                    'not_handled')
        self.assertEqual(loadedTask.jobs[1].runningJob['processStatus'], 
                                                    'not_handled')
        self.assertEqual(loadedTask.jobs[2].runningJob['processStatus'], 
                                                    'in_progress')
        self.assertEqual(loadedTask.jobs[3].runningJob['processStatus'], 
                                                    'not_handled')
        
        self.assertEqual(loadedTask.jobs[0].runningJob['submission'], 
                                    loadedTask.jobs[0]['submissionNumber'])
        self.assertEqual(loadedTask.jobs[1].runningJob['submission'], 
                                    loadedTask.jobs[1]['submissionNumber'])
        self.assertEqual(loadedTask.jobs[2].runningJob['submission'], 
                                    loadedTask.jobs[2]['submissionNumber'])
        self.assertEqual(loadedTask.jobs[3].runningJob['submission'], 
                                    loadedTask.jobs[3]['submissionNumber'])

        jobList = testAPI.loadJobsByRunningAttr(
                            binds = {'status': 'T'} )

        self.assertEqual(len(jobList), 2)
        
        tmpJ = ['top.out', 'date.out']
        self.assertTrue(jobList[0]['standardOutput'] in tmpJ )
        self.assertTrue(jobList[1]['standardOutput'] in tmpJ )
        tmpRJ = ['cacao', 'cippirimerlo']
        self.assertTrue(jobList[0].runningJob['service'] in tmpRJ )
        self.assertTrue(jobList[1].runningJob['service'] in tmpRJ )
        
        jobList = testAPI.loadJobsByRunningAttr(
                            binds = {'status': 'S'} )
        
        self.assertEqual(len(jobList), 1)
        self.assertEqual(jobList[0]['standardError'], 'uname.err')
        self.assertTrue(jobList[0].runningJob['service'], 'ramato' )
        
        jobList = testAPI.loadJobsByRunningAttr(
                            binds = {'processStatus': 'not_handled'} )
        
        self.assertEqual(len(jobList), 3)
        tmp = ['hostname.out', 'uname.out', 'date.out']
        self.assertTrue(jobList[0]['standardOutput'] in tmp)
        self.assertTrue(jobList[1]['standardOutput'] in tmp)
        self.assertTrue(jobList[2]['standardOutput'] in tmp)
        tmpRJ = ['ramato', 'vattelapesca', 'cippirimerlo']
        self.assertTrue(jobList[0].runningJob['service'] in tmpRJ )
        self.assertTrue(jobList[1].runningJob['service'] in tmpRJ )
        self.assertTrue(jobList[2].runningJob['service'] in tmpRJ )
        
        jobList = testAPI.loadJobsByRunningAttr(
                            binds = {'processStatus': 'handled', } )
        
        self.assertEqual(len(jobList), 0)
        
        jobList = testAPI.loadJobsByRunningAttr( binds = 
                            { 'processStatus': 'not_handled', 'status': 'T'} )
        
        self.assertEqual(len(jobList), 1)
        self.assertEqual(jobList[0]['standardError'], 'date.err')
        self.assertTrue(jobList[0].runningJob['service'], 'cippirimerlo' )
            
        return
    
    
    def testE_CombinedMethods(self):
        """
        Test Combined Methods
        """

        testAPI = BossLiteAPI()

        # First create a job
        task = Task()
        task.create(testAPI.db)
        job = Job(parameters = {'name': 'Hadrian', 
                                'jobId': 101, 
                                'taskId': task.exists(testAPI.db),
                                'wmbsJobId': 1 })
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
        Test loadCreated, loadSubmitted, loadEnded and loadFailed
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
                                'jobId': 1,
                                'wmbsJobId': 1 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'W'} )
        self.assertNotEqual(job.runningJob, None)  
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Fringe', 
                                'taskId': tmp, 
                                'jobId': 2,
                                'wmbsJobId': 2 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'SD'} )
        self.assertNotEqual(job.runningJob, None)  
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Stargate Universe', 
                                'taskId': tmp, 
                                'jobId': 3,
                                'wmbsJobId': 3 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'A'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Caprica', 
                                'taskId': tmp, 
                                'jobId': 4,
                                'wmbsJobId': 4 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                            runningAttrs = {'status' : 'K'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'The Mentalist', 
                                'taskId': tmp, 
                                'jobId': 5,
                                'wmbsJobId': 5  } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
                                      runningAttrs = {'closed' : 'Y'} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        task.save(testAPI.db)
         
        result = testAPI.loadCreated()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Spartacus')  
        
        result = testAPI.loadSubmitted()
        self.assertEqual(len(result), 4)
        for x in result : 
            self.assertNotEqual(x['name'], 'The Mentalist') 
        
        result = testAPI.loadEnded()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Fringe') 
        
        result = testAPI.loadFailed()
        self.assertEqual(len(result), 2)
        
        self.assertEqual(result[0]['name'], 'Stargate Universe') 
        self.assertEqual(result[1]['name'], 'Caprica') 
        
        return
    

    def testG_LoadJobRange(self):
        """
        Test loadTask using jobRange
        """

        testAPI = BossLiteAPI()
        
        task = populateDb(testAPI.db, numtask= 1, numjob= 10)
        
        testAPI.saveTask(task)
        
        loadedTask = testAPI.loadTask(taskId = 1 )
        self.assertEqual(len(loadedTask.jobs), 10)
        # print loadedTask.jobIndex
        
        loadedTask = testAPI.loadTask(taskId = 1, jobRange='1' )
        self.assertEqual(len(loadedTask.jobs), 1)
        # print loadedTask.jobIndex
                         
        loadedTask = testAPI.loadTask(taskId = 1, jobRange='10' )
        self.assertEqual(len(loadedTask.jobs), 1)
        # print loadedTask.jobIndex
        
        loadedTask = testAPI.loadTask(taskId = 1, jobRange='1,2,3,4,5' )
        self.assertEqual(len(loadedTask.jobs), 5)
        # print loadedTask.jobIndex
        
        loadedTask = testAPI.loadTask(taskId = 1, jobRange='1:5' )
        self.assertEqual(len(loadedTask.jobs), 5)
        # print loadedTask.jobIndex
        
        loadedTask = testAPI.loadTask(taskId = 1, jobRange='1:3,8:10' )
        self.assertEqual(len(loadedTask.jobs), 6)
        # print loadedTask.jobIndex
        
        return


    def testH_ByTimestamp(self):

        testAPI = BossLiteAPI()

        # Initial time offsets
        tmp = int(time.time())
        timeA = tmp - 30*60
        timeB = tmp - 10*60
        timeZ = tmp

        # First create a job
        task = Task()
        task.save(testAPI.db)
        
        tmp = task.exists(testAPI.db)
        job = Job(parameters = {'name': 'Spartacus', 
                                'taskId': tmp, 
                                'jobId': 1,
                                'wmbsJobId': 1 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
            runningAttrs = {'outputRequestTime' : timeA - 30*60,
                            'outputEnqueueTime' : timeB } )
        self.assertNotEqual(job.runningJob, None)  
        # print job.runningJob
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Fringe', 
                                'taskId': tmp, 
                                'jobId': 2,
                                'wmbsJobId': 2 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
            runningAttrs = {'outputRequestTime' : timeA - 50*60,
                            'outputEnqueueTime' : timeB - 90*60 })
        self.assertNotEqual(job.runningJob, None)  
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Stargate Universe', 
                                'taskId': tmp, 
                                'jobId': 3,
                                'wmbsJobId': 3 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
            runningAttrs = {'outputRequestTime' : timeA ,
                            'outputEnqueueTime' : timeB -30*60 } )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'Caprica', 
                                'taskId': tmp, 
                                'jobId': 4,
                                'wmbsJobId': 4 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
            runningAttrs = {'outputRequestTime' : timeA - 80*60 ,
                            'outputEnqueueTime' : timeB - 110*60} )
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        job = Job(parameters = {'name': 'The Mentalist', 
                                'taskId': tmp, 
                                'jobId': 5,
                                'wmbsJobId': 5 } )
        self.assertEqual(job.runningJob, None)   
        testAPI.getNewRunningInstance(job = job, 
            runningAttrs = {'outputRequestTime' : timeA - 130*60,
                            'outputEnqueueTime' : timeB - 110*60 })
        self.assertNotEqual(job.runningJob, None) 
        task.addJob(job)
        
        task.save(testAPI.db)
        
        
        now = int(time.time())
        
        time_binds = {'outputRequestTime' : [timeZ, now ] }
        jobList = testAPI.loadJobsByTimestamp( time_binds = time_binds, 
                                                    standard_binds = {})
        # According to time_binds I expect 0 jobs
        self.assertEqual(len(jobList), 0)
        
        
        time_binds = {'outputEnqueueTime' : [timeB, now ] }
        jobList = testAPI.loadJobsByTimestamp( time_binds = time_binds, 
                                                    standard_binds = {})
        # According to time_binds I expect 1 jobs
        self.assertEqual(len(jobList), 1)
        
        
        time_binds = {'outputRequestTime' : [timeZ - 50*60, now ],
                      'outputEnqueueTime' : [timeB, now ] }
        jobList = testAPI.loadJobsByTimestamp( time_binds = time_binds, 
                                                    standard_binds = {})
        # According to time_binds I expect 0 jobs
        self.assertEqual(len(jobList), 0)
        
        return
      
    
if __name__ == "__main__":
    APIsuite = unittest.TestLoader().loadTestsFromTestCase(APITest)

    # run the unit-test
    unittest.TextTestRunner(verbosity=3).run(APIsuite)
