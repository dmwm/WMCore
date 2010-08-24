#!/usr/bin/env python
"""
_JobGroup_t_

Test creates one WMBS database instance which is used for all tests.

"""

__revision__ = "$Id: JobGroup_t.py,v 1.2 2008/10/28 18:18:21 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands
from sets import Set
#from WMCore_t.DataStructs_t.JobGroup_t import JobGroup_t
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Fileset import Fileset 
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription 
from WMCore.WMBS.Job import Job
from sets import Set

from unittest import TestCase
import logging
import random

import time
from datetime import datetime

class JobGroupTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        self.logger = logging.getLogger('JobGroupTest')
        
        self.dbfile = 'sqlite:///jobgrouptest.lite'
        self.dbfile = 'mysql://metson@localhost/wmbs'
        self.tearDown()
        
        self.dbf = DBFactory(self.logger, self.dbfile)
        self.daofactory = DAOFactory(package='WMCore.WMBS', 
                                      logger=self.logger, 
                                      dbinterface=self.dbf.connect())
        
        creator = self.daofactory(classname='CreateWMBS')
        creator.execute()
        #assert createworked, "create output: %s" % createworked 
    
    def tearDown(self):
        #Should really move the file...
        #stamp = time.mktime(datetime.now().timetuple())
        #dbfile = 'sqlite:///jobgrouptest_%s.lite' % stamp
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.logger.debug("WMBS MySQL database deleted")
        try:
            os.remove(self.dbfile.replace('sqlite:///', ''))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        
    def testInit(self):
        fileset = Fileset(name='jobgrouptest_files', 
                          logger=self.logger, 
                          dbfactory=self.dbf)
        workflow = Workflow(spec='/home/metson/workflow.xml', 
                                     owner='metson', 
                                     name='My Analysis', 
                                     logger=self.logger, 
                                     dbfactory=self.dbf)
        fileset.create()
        workflow.create()
        
        l = []
        for i in range(0,10):
            lfn = '/store/data/%s/%s/file%s.root' % (random.randint(1000, 9999), 
                                      random.randint(1000, 9999), i)
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
            
            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi,
                        logger=self.logger, dbfactory=self.dbf)
            file.save()
            file.load()
            l.append(file)
            fileset.addFile(file)
        
        fileset.commit()
        print "Fileset made"    
        
        sub = Subscription(fileset=fileset, workflow=workflow,
                        logger=self.logger, dbfactory=self.dbf)
        sub.create()
        print "Subscription made"
        
        # pretend we've got the job group from a factory
        set = Set()
        group = JobGroup(subscription = sub)
        
        for i in l:
            fs = Fileset(name='tmp', logger=self.logger, dbfactory=self.dbf)
            fs.addFile(i)
            job = Job(files=fs, logger=self.logger, dbfactory=self.dbf)
            set.add(job)
            
        group.add(set)
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
                    j.submit()
                if j.status == 'ACTIVE':
                    dice = random.randint(0 , 10) / 10.
                    if dice <= complete_prob:
                        self.logger.debug( "#job completes" )
                        j.complete('complete report')
                        j.addOutput(File(lfn='iter%s_job%s' % (i, jobs.index(j)), \
                                     logger=self.logger, dbfactory=self.dbf))                 
                    elif random.randint(0 , 10) / 10. < fail_prob:
                        self.logger.debug(  "#job fails" )
                        j.fail('fail report')
                
        print group.output().listLFNs()
        
    def testOutput(self):
        group = self.testInit()
        
    
    def testStatus(self):
        group = self.testInit()
    
if __name__ == '__main__':
    unittest.main()