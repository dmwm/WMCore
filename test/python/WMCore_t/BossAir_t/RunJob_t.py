#!/usr/bin/env python

"""
BossAir preliminary test
"""

import unittest
import threading


import WMCore.WMInit
from WMQuality.TestInit             import TestInit
from WMCore.DAOFactory              import DAOFactory

from WMCore.WMBS.Job          import Job
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow


from WMCore.BossAir.RunJob    import RunJob


class RunJobTest(unittest.TestCase):
    """
    _RunJobTest_

    
    Test the RunJob object and accessors
    """


    def setUp(self):
        
        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"],
                                useDefault = False)

        self.daoFactory = DAOFactory(package = "WMCore.BossAir",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)



    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"])

        self.testInit.delWorkDir()

        return


    def createJobs(self, nJobs):
        """
        Creates a series of jobGroups for submissions

        """

        testWorkflow = Workflow(spec = "dummy", owner = "mnorman",
                                name = "dummy", task="basicWorkload/Production")
        testWorkflow.create()

        # Create Fileset, Subscription, jobGroup
        testFileset = Fileset(name = "dummy")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()


        # Create jobs
        for id in range(nJobs):
            testJob = Job(name = 'Job_%i' % (id))
            testJob['owner'] = "mnorman"
            testJob.create(testJobGroup)
            testJobGroup.add(testJob)

        testFileset.commit()
        testJobGroup.commit()

        
        return testJobGroup
            




    def testA_BulkDAOs(self):
        """
        _BulkDAOs_

        Test the bulk DAO options, which is the only thing we should be using.
        """

        myThread = threading.currentThread()

        jobGroup = self.createJobs(nJobs = 10)

        runJobs = []
        for job in jobGroup.jobs:
            runJob = RunJob(jobid = job.exists())
            runJob['status'] = 'New'
            runJob['user']   = job['owner'] 
            runJobs.append(runJob)


        statusDAO = self.daoFactory(classname = "NewState")
        statusDAO.execute(states = ['New', 'Gone', 'Dead'])

        result = myThread.dbi.processData("SELECT name FROM bl_status")[0].fetchall()
        self.assertEqual(result, [('Dead',), ('Gone',), ('New',)])


        newJobDAO = self.daoFactory(classname = "NewJobs")
        newJobDAO.execute(jobs = runJobs)


        result = myThread.dbi.processData("SELECT wmbs_id FROM bl_runjob")[0].fetchall()
        self.assertEqual(result,
                         [(1L,), (2L,), (3L,), (4L,), (5L,), (6L,), (7L,), (8L,), (9L,), (10L,)])


        loadJobsDAO = self.daoFactory(classname = "LoadByStatus")
        loadJobs = loadJobsDAO.execute(status = "New")
        self.assertEqual(len(loadJobs), 10)
        

        idList = [x['id'] for x in loadJobs]

        for job in loadJobs:
            job['bulkid'] = 1001

        updateDAO = self.daoFactory(classname = "UpdateJobs")
        updateDAO.execute(jobs = loadJobs)

        loadJobs = loadJobsDAO.execute(status = 'New')
        self.assertEqual(len(loadJobs), 10)
        for job in loadJobs:
            self.assertEqual(job['bulkid'], '1001')


        loadWMBSDAO = self.daoFactory(classname = "LoadByWMBSID")
        for job in jobGroup.jobs:
            jDict = loadWMBSDAO.execute(jobs = [job])
            self.assertEqual(job['id'], jDict[0]['jobid'])
        

        setStatusDAO = self.daoFactory(classname = "SetStatus")
        setStatusDAO.execute(jobs = idList, status = 'Dead')

        result = loadJobsDAO.execute(status = 'Dead')
        self.assertEqual(len(result), 10)
        result = loadJobsDAO.execute(status = 'New')
        self.assertEqual(len(result), 0)

        runningJobDAO = self.daoFactory(classname = "LoadRunning")
        runningJobs = runningJobDAO.execute()

        self.assertEqual(len(runningJobs), 10)

        completeDAO = self.daoFactory(classname = "CompleteJob")
        completeDAO.execute(jobs = idList)

        result = loadJobsDAO.execute(status = 'Dead', complete = '0')
        self.assertEqual(len(result), 10)


        deleteDAO = self.daoFactory(classname = "DeleteJobs")
        deleteDAO.execute(jobs = idList)

        result = loadJobsDAO.execute(status = 'Dead')
        self.assertEqual(len(result), 0)
        


        return
        
        
    def testB_CheckWMBSBuild(self):
        """
        _CheckWMBSBuild_

        Trivial test that checks whether we can build
        runJobs from WMBS jobs
        """

        jobGroup = self.createJobs(nJobs = 10)

        for job in jobGroup.jobs:
            rj = RunJob()
            rj.buildFromJob(job = job)
            self.assertEqual(job['id'], rj['jobid'])
            self.assertEqual(job['retry_count'], rj['retry_count'])
            job2 = rj.buildWMBSJob()
            self.assertEqual(job['id'], job2['id'])
            self.assertEqual(job['retry_count'], job2['retry_count'])


        return


        

if __name__ == '__main__':
    unittest.main()


