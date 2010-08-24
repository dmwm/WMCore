#!/usr/bin/env python
"""
_FileBased_

Unit tests for job splitting Factories. Should be one test per algorithm, 
and one per job type.

"""

__revision__ = "$Id: FileBased_t.py,v 1.1 2008/09/25 13:14:01 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
from sets import Set
import unittest, logging, os, commands, random, datetime, math
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Workflow import Workflow

from WMCore.WMBS.Job import Job as WMBSJob
from WMCore.WMBS.Subscription import Subscription as WMBSSubscription
from WMCore.WMBS.File import File as WMBSFile
from WMCore.WMBS.Fileset import Fileset as WMBSFileset
from WMCore.WMBS.Workflow import Workflow as WMBSWorkflow

from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory

class FileBasedGenericObjectTest(unittest.TestCase):
    """
    A test of the job splitting algorithm "FileBased" using generic WMObjects
    """    
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
        self.testlogger = logging.getLogger('wmbs_FileBasedGenericObjectTest')
        self.fileset = Fileset(name='MyCoolFiles', logger = self.testlogger)
        
        for i in range(0, 993):
            file = File("/store/data/Electrons/1234/5678/k123ljhkj2%s.root" % i, 
                             1000, 2000, 10 + i, 12312)
            self.fileset.addFile(file)
        self.fileset.commit()
        work = Workflow()
        self.subscription = Subscription(fileset = self.fileset, 
                workflow = work, split_algo = 'FileBased', type = "Processing")
        
        assert len(self.subscription.getFileset().listFiles()) == \
                len(self.subscription.availableFiles())
    
    def tearDown(self):
        pass
    
    def testMakeJobs(self):
        files_size = len(self.fileset.listFiles())
        print "Number of files: %i" % files_size
        assert len(self.subscription.getFileset().listFiles()) == \
                len(self.subscription.availableFiles())
        assert len(self.subscription.availableFiles()) == files_size
        
        splitsize = 89
        splitter = SplitterFactory()
        
        jobfactory = splitter(self.subscription)
        jobgroup = jobfactory(files_per_job=splitsize)

        assert len(self.subscription.getFileset().listFiles()) == files_size
        assert len(self.subscription.availableFiles()) == 0

        print len(self.fileset.listFiles()), \
                    len(self.subscription.getFileset().listFiles())
        print files_size, splitsize, len(jobgroup.jobs)
        
        number_jobs = divmod(files_size, splitsize)
        print "should have %i jobs of %i files and 1 job of %i files" % \
                    (number_jobs[0], splitsize, number_jobs[1])
        job_test = 0 
        if number_jobs[1] > 0:
            job_test = 1
        job_test = job_test + number_jobs[0]
        assert job_test == len(jobgroup.jobs), "Factory made the wrong number of jobs"
        c = 0
        i = 0
        for job in jobgroup.jobs:
            i = i + 1
            print "job %i : %i files" % (i, len(job.listFiles()))
            c = c + len(job.listFiles())
            assert len(job.listFiles()) <= splitsize, \
                    "Job has more files than it should"
        print c
        assert c == files_size, "Jobs will run on the wrong number of files"
        
        # Now check that jobs have different files and that they have all 
        # files in the original fileset.
        #
        # s symmetric_difference t = new set with elements in either s or t but 
        # not both.
        #
        # So make a new Set using the symmetric_difference of the new (empty)
        # Set and the file_set of each job. If all files for a particular job
        # are added to test_set and by the end test_set contains all the file
        # we are good to go!
        
        test_set = Set()
        for job in jobgroup.jobs:
            test_set_len = len(test_set)
            job_set_len = len(job.file_set)
            test_set = job.listFiles() ^ test_set
            assert len(test_set) == test_set_len + job_set_len
        assert len(test_set) == files_size
        
class FileBasedWMBSObjectTest(FileBasedGenericObjectTest):
    """
    A test of the job splitting algorithm "FileBased" using WMBS Objects
    """
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
        self.testlogger = logging.getLogger('wmbs_FileBasedWMBSObjectTest')
        self.tearDown()
        
        self.dbf = DBFactory(logging.getLogger('wmbs_mysql'), 
                             'mysql://metson@localhost/wmbs')
        daofactory = DAOFactory(package='WMCore.WMBS', 
                                logger=logging.getLogger('wmbs_sql'), 
                                dbinterface=self.dbf.connect())
        
        theCreator = daofactory(classname='CreateWMBS')
        assert not theCreator.execute(), "could not create database instance"

        self.fileset = WMBSFileset(name='MyCoolFiles', 
                                   logger=logging.getLogger('wmbs_fileset'), 
                                   dbfactory=self.dbf)

        self.fileset.create()

        filecreate = datetime.datetime.now()
        print "Creating files - %s" % filecreate
        
        for i in range(0, 993):
            #if i/50. == i/50:
            #    print i
            file = WMBSFile(lfn="/store/data/Electrons/1234/5678/h1%s.root" % i, 
                             size=1000, events=2000, lumi=10 + i, run=12312, 
                             logger=logging.getLogger('wmbs_file'), 
                             dbfactory=self.dbf)

            self.fileset.addFile(file)
            
        startsave = datetime.datetime.now()
        print "Saving Fileset - %s" % startsave
        self.fileset.commit()
        complete = datetime.datetime.now()
        assert len(self.fileset.listLFNs()) == 993, "Fileset has wrong number of files: %s"\
                            % len(self.fileset.listLFNs())
        work = WMBSWorkflow(spec='coolworkflow0001.xml', owner='JoeBloggs', 
                            name='My Cool Workflow',
                            logger=logging.getLogger('wmbs_workflow'), 
                            dbfactory=self.dbf)
        work.create()
        assert work.exists()
        self.subscription = WMBSSubscription(fileset = self.fileset, 
                workflow = work, 
                split_algo = 'FileBased', 
                type = "Processing", 
                logger=logging.getLogger('wmbs_subscription'), 
                dbfactory=self.dbf)
        self.subscription.create()
        
        assert self.subscription.exists()
        
        assert len(self.subscription.getFileset().listFiles()) == \
                len(self.subscription.availableFiles())
    
    def tearDown(self):
       # commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs')
       # commands.getstatusoutput('mysqladmin -u root create wmbs')
        try:
            self.testlogger.debug(os.remove('FileBasedWMBSObjectTest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.testlogger.debug("WMBS SQLite database deleted")
    
            
if __name__ == "__main__":
    unittest.main()     
