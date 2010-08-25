from WMComponent.JobStatusLite.JobStatusPoller import JobStatusPoller
from WMComponent.JobStatusLite.StatusScheduling import StatusScheduling
from WMQuality.TestInit import TestInit
# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI

from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM
from WMCore.BossLite.Common.Exceptions  import DbError

import threading
import unittest
import os

class JobStatusLite_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.BossLite"], \
                                useDefault = False)

    def tearDown(self):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return

    def fillDatabase(self, numtask = 2, numjob = 50, status = 'R', p_status = 'handled'):
        print "Populating database"
        db = BossLiteDBWM()
        count_job = 0
        names = []
        for t in xrange(numtask):
            try:
                task = Task()
                task.data['name'] = 'task_%s'%str(t)
                names.append('task_%s'%str(t))
                task.create(db)
                for j in xrange(1, 1+numjob):
                    job_static = { \
                                   'name':      '%s_job_%s'%(str(t),str(j)), \
                                   'jobId':     j, \
                                   'taskId':    task.exists(db), \
                                   'submissionNumber': 1, \
                                   'closed':    'N' \
                                 }
                    job = Job(parameters = job_static)
                    job.create(db)
                    job.save(db)
                    job_run = { \
                                'jobId':         job.data['jobId'], \
                                'taskId':        task.exists(db), \
                                'submission':    1, \
                                'schedulerId':   'id_scheduler', \
                                'processStatus': p_status, \
                                'closed':        'N', \
                                'status':        status \
                               }
                    runJob = RunningJob(parameters = job_run)
                    runJob.create(db)
                    runJob.save(db)
                    count_job += 1
            except Exception, ex:
                print "ERROR: '%s'"%str(ex)
                print "\ttask_" +str(t)
        print "..finished."
        return names, count_job


    def createConfig(self):
        config = self.testInit.getConfiguration()

        config.component_('JobStatusLite')
        config.JobStatusLite.namespace       = 'WMComponent.JobStatusLite.JobStatusLite'
        config.JobStatusLite.componentDir    = os.getcwd()
        config.JobStatusLite.ComponentDir    = '/tmp/application/JobStatusLite'
        config.JobStatusLite.logLevel        = 'INFO'
        config.JobStatusLite.pollInterval    = 60
        config.JobStatusLite.queryInterval   = 12
        config.JobStatusLite.jobLoadLimit    = 500
        config.JobStatusLite.maxJobQuery     = 50
        config.JobStatusLite.taskLimit       = 30
        config.JobStatusLite.maxJobsCommit   = 100
        config.JobStatusLite.processes       = 3

        return config


    def testA_PollingFailed(self):
        config = self.createConfig()
        t_added, n_job = self.fillDatabase(1, 10, 'A', 'handled')
        print "Calling JobStatusPoller"
        obj1 = JobStatusPoller(config)
        obj1.setup(None)
        obj1.algorithm(None)
        obj1.terminate(None)
        print "..finished."
        print "Checking jobs were processed" 
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName(t_added[0])
        for job in task.jobs:
            self.assertEqual(job.runningJob['processStatus'], 'failed')

    def testB_PollingSuccess(self):
        config = self.createConfig()
        t_added, n_job = self.fillDatabase(1, 10, 'SD', 'handled')
        print "Calling JobStatusPoller"
        obj1 = JobStatusPoller(config)
        obj1.setup(None)
        obj1.algorithm(None)
        obj1.terminate(None)
        print "..finished."
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName(t_added[0])
        for job in task.jobs:
            self.assertEqual(job.runningJob['processStatus'], 'output_requested')

    def testC_PollingNew(self):
        config = self.createConfig()
        t_added, n_job = self.fillDatabase(1, 10, 'S', 'not_handled')
        print "Calling JobStatusPoller"
        obj1 = JobStatusPoller(config)
        obj1.setup(None)
        obj1.algorithm(None)
        obj1.terminate(None)
        print "..finished."
        print "Checking jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName(t_added[0])
        for job in task.jobs:
            self.assertEqual(job.runningJob['processStatus'], 'handled')

    def testD_PollingKilled(self):
        config = self.createConfig()
        t_added, n_job = self.fillDatabase(1, 10, 'K', 'handled')
        print "Calling JobStatusPoller"
        obj1 = JobStatusPoller(config)
        obj1.setup(None)
        obj1.algorithm(None)
        obj1.terminate(None)
        print "..finished."
        print "Checking jobs were processed"
        db = BossLiteDBWM()
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName(t_added[0])
        for job in task.jobs:
            self.assertEqual(job.runningJob['processStatus'], 'failed')


    def testZ_GroupAssignment(self):
        config = self.createConfig()
        t_added, n_job = self.fillDatabase(5,50)
        print "Calling StatusScheduling"
        obj1 = StatusScheduling(config)
        obj1.setup(None)
        obj1.algorithm(None)
        obj1.terminate(None)
        print "..finished."
        print "Checking jobs were processed"
        db = BossLiteDBWM()

        ## check if jobs have been selected as new jobs
        ## check if jobs have been assigned to a group
        result = db.executeSQL("SELECT j.group_id, j.job_id, j.task_id " +\
                               "FROM jt_group j JOIN bl_runningjob b ON " +\
                               "(j.task_id = b.task_id and j.job_id = b.job_id) " +\
                               "order by j.group_id, j.task_id, j.job_id;" \
                              )
        raws = result[0].fetchall()
        self.assertEqual(len(raws), n_job)
        for tupla in raws:
            group, task, job = tupla
            self.assertNotEqual(group, 0)
        ## check if job status has been updated 
        ## No check to do till BossLiteAPI are not available

if __name__ == '__main__':
    unittest.main()

