#!/usr/bin/env python
"""
_Job_t_

Testcase for the Job class

""" 

import unittest, logging, random
from sets import Set
from WMCore.DataStructs.Job import Job 
from WMCore.DataStructs.Pickleable import Pickleable 
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from unittest import TestCase

class JobTest(unittest.TestCase):
    """
    _JobTest_

    Testcase for the Job class

    Instantiate a dummy Job object with a dummy Subscription 
    and a dummy Fileset full of random files as input
    
    """

    def setUp(self):
        """
        Initial Setup for the Job Testcase
        
        """
        #Setting a Dummy Subscription for the Job
        self.dummyFile = File('/tmp/dummyfile',9999,0,0,0,0)
        self.dummySet = Set() 
        self.dummySet.add(self.dummyFile)
        self.dummyFileSet = Fileset(name = 'SubscriptionTestFileset', files = self.dummySet)
        self.dummyWorkFlow = Workflow()
        self.dummySubscription = Subscription(fileset = self.dummyFileSet, workflow = self.dummyWorkFlow)
        #Setting a dummyFileset for the Job
        self.dummyFileSet2 = Fileset(name = 'JobTestFileset')		
        for i in range(1,1000):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                          random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, cksum = 1)
            file.addRun(Run(1, *[45]))
            self.dummyFileSet2.addFile(file)
		
        self.dummyJob = Job(files=self.dummyFileSet2)
	
    def tearDown(self):
        """
        No tearDown method for this Testcase

        """
        pass

    def testListFiles(self):
        """
        Testcase for the listFiles method of the Subscription Class
        """
        assert self.dummyJob.listFiles() == self.dummyFileSet2.listFiles(),'Initial fileset does\'nt match Job fileset - listFiles method mismatch'

    def testListLFNs(self):
        """
        Testcase for the listLFNs method of the Subscription Class
        """
        assert self.dummyJob.listLFNs() == self.dummyFileSet2.listLFNs(),'Initial fileset does\'nt match fileset - listLFNs method mismatch'

    def testaddFile(self):
        """
        Testcase for the addFile method of the Subscription Class
        """
        dummyFileAddFile = File('/tmp/dummyFileAddFileTest',1234,1,2,3,4)
        self.dummyJob.addFile(dummyFileAddFile)
        assert dummyFileAddFile in self.dummyJob.file_set.listFiles(), 'Couldn\'t add file to Job - addFile method error'

    def testaddOutput(self):
        """
        Testcase for the addOutput method of the Subscription Class
        """
        dummyFileAddOutput = File('/tmp/dummyFileAddOutputTest',1234,1,2,3,4)
        self.dummyJob.addOutput(dummyFileAddOutput)
        assert dummyFileAddOutput in self.dummyJob.output.listFiles(), 'Couldn\'t add output file to Job - addOutput method error'

    def testChangeStatus(self):
        """
        Testcase for the changeStatus method of the Subscription Class
        """
        timeNow = self.dummyJob.last_update
        self.dummyJob.changeStatus('TEST')
        assert self.dummyJob.last_update != timeNow,'Couldn\'t modify last update time for the job - changeStatus method error'
        assert self.dummyJob.status == 'TEST','Couldn\'t change Job status - changeStatus method error'

    def testSubmit(self):
        """
        Testcase for the submit method of the Subscription Class
        """
        self.dummyJob.submit(name="batch queue id")
        assert self.dummyJob.status == 'ACTIVE','couldn\'t change Job status to ACTIVE - submit method error'

    def testResubmit(self):
        """
        Testcase for the resubmit method of the Subscription Class
        """
        self.dummyJob.resubmit(name="batch queue id")
        assert self.dummyJob.status == 'ACTIVE','couldn\'t change Job status to ACTIVE - resubmit method error'
		
    def testFail(self):
        """
        Testcase for the fail method of the Subscription Class
        """
        self.dummyJob.fail('JOB FAIL TEST')
        assert self.dummyJob.status == 'FAILED','couldn\'t change Job status to FAILED - fail method error'
        assert self.dummyJob.report == 'JOB FAIL TEST', 'couldn\'t write report correctly - fail method error'

    def testComplete(self):
        """
        Testcase for the complete method of the Subscription Class
        """
        self.dummyJob.complete('JOB COMPLETE TEST')
        assert self.dummyJob.status == 'COMPLETE','couldn\'t change Job status to COMPLETED - complete method error'
        assert self.dummyJob.report == 'JOB COMPLETE TEST', 'couldn\'t write report correctly - complete method error'
		

if __name__ == '__main__':
    unittest.main()
