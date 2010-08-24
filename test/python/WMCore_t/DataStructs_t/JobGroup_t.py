from WMCore.DataStructs.Fileset import Fileset 
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Subscription import Subscription 
from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.JobGroup import JobGroup
from sets import Set

import unittest
import logging
import random

class JobGroupTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        self.logger = logging.getLogger('JobGroupTest')
        
         
    def testInit(self):
        fileset = Fileset(logger=self.logger)
        l = []
        for i in range(0,10):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
            
            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi)
            fileset.addFile(file)
            if i < 11:
                l.append(file)
        
        print "Fileset made"    
        
        sub = Subscription(fileset=fileset, workflow='/my/test/workflow')
        print "Subscription made"
        
        # pretend we've got the job group from a factory
        set = Set()
        for i in l:
            fs = Fileset(name='tmp', logger=self.logger)
            fs.addFile(i)
            job = Job(files=fs)
            set.add(job)
            
        group = JobGroup(subscription = sub, jobs=set) 
        return group
    
    def testFileCycle(self):
        print "testFileCycle"
        group = self.testInit()
        jobs = list(group.jobs)
        maxacquire = 10
        fail_prob = 1/4.
        complete_prob = 1/2.
        print "probabilities: complete %s, fail %s" % (complete_prob, fail_prob)
        i=0
        while group.status() != 'COMPLETE':
            i = i + 1
            print "group status: %s" % group.status(detail=True)
            # Decide if jobs have completed or failed
            for j in jobs:
                if j.status in ['QUEUED', 'FAILED']:
                    print "submitting"
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
                
        print group.output().listLFNs()
        
    def testOutput(self):
        group = self.testInit()
    
if __name__ == '__main__':
    unittest.main()