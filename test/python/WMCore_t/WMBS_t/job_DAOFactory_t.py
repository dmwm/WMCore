#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: job_DAOFactory_t.py,v 1.4 2009/05/09 12:05:27 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import unittest, logging, os, commands, random, datetime
from sets import Set
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Actions.Files.FullAdd import FullAddAction
#pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py

class BaseJobsTestCase(unittest.TestCase):
    def setUp(self):
        "make a logger instance"
        #level=logging.ERROR
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')
        
        self.mysqllogger = logging.getLogger('wmbs_mysql')
        self.sqlitelogger = logging.getLogger('wmbs_sqlite')
        self.testlogger = logging.getLogger('unit_test')
        
        self.tearDown()
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
        
        self.selist = ['lcgse01.phy.bris.ac.uk', 'lcgse02.phy.bris.ac.uk', 'se01.fnal.gov', 'se02.fnal.gov']
        
        for se in self.selist:
            self.daofactory1(classname='Locations.New').execute(sename=se)
            self.daofactory2(classname='Locations.New').execute(sename=se)
        
    def tearDown(self):
        """
        Delete the databases
        """
        self.testlogger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.testlogger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.testlogger.debug("WMBS MySQL database deleted")
        try:
            self.testlogger.debug(os.remove('filesettest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.testlogger.debug("WMBS SQLite database deleted")
    
    
class JobBusinessObjectTestCase(BaseJobsTestCase):
    def setUp(self):
        BaseJobsTestCase.setUp(self)
        testlogger = logging.getLogger('setUp')
        c = 0
        self.workflow = []
        self.fileset = []
        for dbi in self.dbf1, self.dbf2:
            self.workflow.append(Workflow(spec='/home/metson/workflow.xml', 
                                 owner='metson', 
                                 name='My Analysis', 
                                 task="Test",
                                 logger=testlogger, 
                                 dbfactory=dbi))
            self.workflow[c].create()
            
            self.fileset.append(Fileset(name='MyCoolFiles', logger=testlogger, 
                                 dbfactory=dbi))
            self.fileset[c].create()
            c = c + 1 
            self.daofactory1(classname='Files.AddToFileset')
            
        filelist = []
        num_files = 1000
        for i in range(0,num_files):
            filelist.append(("/store/data/Electrons/setup%s.root" % i, 
                             1000, 2000, 10 + i, 12312))
        adder = FullAddAction(testlogger)
        for dao in self.daofactory1, self.daofactory2:
            adder.execute(files=filelist,
                                  daofactory = dao)
            def strim(tuple): return tuple[0]
            filelist = map(strim, filelist)
            dao(classname='Files.AddToFileset').execute(file=filelist, fileset='MyCoolFiles')
            
        self.subscriptions = []
        c = 0
        for dbi in [self.dbf1]:#, self.dbf2:
            self.subscriptions.append(Subscription(fileset = self.fileset[c], 
                                            workflow = self.workflow[c], 
                                            logger=testlogger, 
                                            dbfactory = dbi))
            self.subscriptions[c].create()
            c = c + 1

    def testMakeJob(self):
        testlogger = logging.getLogger('testMakeJob')
        db_interfaces = [self.dbf1]#, self.dbf2:
        for dbi in db_interfaces:
            sub = self.subscriptions[db_interfaces.index(dbi)]
            num_files = len(sub.availableFiles())
            print "files available %s, files acquired %s" % (num_files,len(sub.acquiredFiles()))
            job1 = Job(subscription = sub, logger=testlogger, dbfactory = dbi)
            print "job %s has %i files" % (job1.id, len(job1.listLFNs()))
            before = len(sub.acquiredFiles())
            print "files available %s, files acquired %s" % (len(sub.availableFiles()),before)
            size = 10
            fileset = Fileset(name='testMakeJobFileset', 
                              logger=testlogger, 
                              dbfactory=dbi)
            for i in sub.acquireFiles(size=size):
                file = File(id=i, logger=testlogger, dbfactory=dbi)
                file.load()
                fileset.addFile(file)
            fileset.commit()
            
            job2 = Job(subscription = sub, files = fileset, logger=testlogger, dbfactory = dbi)
            print "job %s has %i files" % (job2.id, len(job2.listLFNs()))
            print "files available %s, files acquired %s" % (len(sub.availableFiles()), len(sub.acquiredFiles()))
            assert len(sub.acquiredFiles()) == before + size, "Job did not acquire files correctly"
            assert len(sub.availableFiles()) == num_files - before - size, "Job did not acquire files correctly"
                        
    def testFileCycle(self):
        testlogger = logging.getLogger('testFileCycle')
        db_interfaces = [self.dbf1]#, self.dbf2:
        for dbi in db_interfaces:
            sub = self.subscriptions[db_interfaces.index(dbi)]
            print "files available %s, files acquired %s" % (len(sub.availableFiles()),len(sub.acquiredFiles()))
            # Make 10 jobs of 100 files each
            size = 100
            jobs = []
            for j in range(0, 10):
                fileset = Fileset(name='testMakeJobFileset_%s' % j, # Need to increment the name as it goes to the DB
                              logger=testlogger, 
                              dbfactory=dbi)
                for i in sub.acquireFiles(size=size):
                    file = File(id=i, logger=testlogger, dbfactory=dbi)
                    file.load()
                    fileset.addFile(file)
                fileset.commit()
                job = Job(subscription = sub, files = fileset, logger=testlogger, dbfactory = dbi)
                jobs.append(job)
                assert len(job.listLFNs()) == size, "Job has a different number of files (%s) than expected (%s)" % (len(job.listLFNs()), size)
                assert job.id == j + 1, "Job id is not what is expected"
            job = Job(subscription = sub, files = sub.acquireFiles(size=size), logger=testlogger, dbfactory = dbi)
            assert len(job.getFiles()) == 0, "11th job has files"
            print "files available %s, files acquired %s" % (len(sub.availableFiles()),len(sub.acquiredFiles()))

    def testFileManip(self):
        testlogger = logging.getLogger('testFileManip')
        db_interfaces = [self.dbf1]#, self.dbf2:
        for dbi in db_interfaces:
            sub = self.subscriptions[db_interfaces.index(dbi)]
            job = Job(sub)
            job.load()
            for i in range(0,10):
                print i, len(job.listLFNs())
                file = File(lfn="/store/data/%s.root" % i, 
                                 size=1000, events=2000, cksum=100, logger=testlogger, dbfactory = dbi)
                print file.dict['id']
                file.save()
                print file.dict['id']
                job.addFile(file)
                #assert len(job.listLFNs()) == i+1, "wrong number of lfn's associated to job: %s not %s" % ( len(job.listLFNs()), i+1)

                job.associateFiles()
                print i, len(job.listLFNs())
                testjob = Job(subscription=sub, id=job.id)
                print i, len(job.listLFNs()), len(testjob.listLFNs())
                assert len(job.listLFNs()) == len(testjob.listLFNs()), "job incorrectly loaded from database"
                print i, len(job.listLFNs())
                
                print dbi.connect().processData("select count(*) from wmbs_job_assoc where job=%s" % job.id)[0].fetchall()
                
                
                
if __name__ == "__main__":
    unittest.main()
    
