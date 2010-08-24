from WMCore.DataStructs.Fileset import Fileset 
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Subscription import Subscription 
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.DataStructs.Run import Run

from WMQuality.TestInit import TestInit

from sets import Set

import unittest
import logging
import random
import os

class JobGroupTest(unittest.TestCase):
    _setup = False
    
    def setUp(self):
        if self._setup:
            return
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self._setup = True
         
    def testSetup(self):
        fileset = Fileset()
        l = []
        for i in range(0,10):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
            
            file = File(lfn=lfn, size=size, events=events, cksum=1)
	    file.addRun(Run(run, *[lumi]))
            fileset.addFile(file)
            if i < 11:
                l.append(file)
        
        sub = Subscription(fileset=fileset, workflow='/my/test/workflow')
        
        # pretend we've got the job group from a factory
        set = Set()
        for i in l:
            fs = Fileset(name='tmp')
            fs.addFile(i)
            job = Job(files=fs)
            set.add(job)
            
        group = JobGroup(subscription = sub, jobs=set) 
        return group
    
    def testFileCycle(self):
        group = self.testSetup()
        jobs = list(group.jobs)
        maxacquire = 10
        fail_prob = 1/4.
        complete_prob = 1/2.
        i=0
        while group.status() != 'COMPLETE':
            i = i + 1
            # Decide if jobs have completed or failed
            for j in jobs:
                if j.status in ['QUEUED', 'FAILED']:
                    j.submit(name='batch queue id')
                if j.status == 'ACTIVE':
                    dice = random.randint(0 , 10) / 10.
                    if dice <= complete_prob:
                        self.logger.debug( "#job completes" )
                        j.complete('complete report')
                        j.addOutput(File(lfn='iter%s_job%s' % (i, jobs.index(j))))                 
                    elif random.randint(0 , 10) / 10. < fail_prob:
                        self.logger.debug(  "#job fails" )
                        j.fail('fail report')
                
    def testOutput(self):
        group = self.testSetup()
    
if __name__ == '__main__':
    unittest.main()
