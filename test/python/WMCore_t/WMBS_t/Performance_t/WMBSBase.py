#!/usr/bin/env python
"""
__WMBSBase__

Base class for Performance Tests at WMBS

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import random, os, threading, os.path

from ConfigParser import ConfigParser

from WMCore_t.Database_t.Performance import Performance
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run
from WMCore.WMFactory import WMFactory
from sets import Set

#Needed for TestInit
from WMQuality.TestInit import TestInit

__revision__ = "$Id: WMBSBase.py,v 1.18 2009/05/13 18:09:43 mnorman Exp $"
__version__ = "$Reivison: $"

class WMBSBase(Performance):
    """
    __WMBSBase__

    Base class for Performance Tests at WMBS

    This class is abstract, proceed to the DB specific testcase
    to run the test


    """

    _setup = False
    _teardown = False

    def genFileObjects(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS File Objects for testing

        """
        # Create a Fileset of random, parentless, childless, unlocatied file
        filelist = []

        #Generating Files
        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        for x in range(rangemax):
            file = File(lfn = '/store/data/'+name+'test'+str(x)+'.root',
                        size = random.randint(1000, 2000),
                        events = 1000)

            filelist.append(file)
        return filelist

    def genFiles(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS File Objects for testing,
        creating them at the DB

        """
        
        
        filelist = self.genFileObjects(number)


        setfiles = Set(filelist)

        fileset = Fileset(name = name+'Files',
                            files = setfiles)

        fileset.create()

        filelist = list(fileset.getFiles())

        return filelist

    def genLocationObjects(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Location Objects for testing

        """

        list = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        for x in range(rangemax):
            list.append(name+'Location'+str(x))

        return list

    def genLocation(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Location Objects for testing,
        creating them at the DB

        """

        list = self.genLocationObjects(number = number, name = name)

        for x in list:
            self.dao(classname = 'Locations.New').execute(siteName = x)      
        
        return list

    def genFilesetObjects(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Fileset Objects for testing

        """

        list = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        
        for i in range(rangemax):        
            filelist = self.genFileObjects(number = 10, name = name+'Fileset')
            fileset = Fileset(name = name+str(i), 
                            files = set(filelist))
            list.append(fileset)        
     
        return list

    def genFileset(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Fileset Objects for testing,
        creating them at the DB

        """
        
        list = self.genFilesetObjects(number, name)

        for x in list:
            x.create()

        return list

    def genWorkflowObjects(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Workflow Objects for testing

        """

        list = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        for i in range(rangemax):        
            workflow = Workflow(spec = name+'Spec'+str(i), 
                                owner = name+'Owner'+str(i), 
                                name = name+'Workflow'+str(i),
                                task="Test"+str(i))
            list.append(workflow)

        return list

    def genWorkflow(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Workflow Objects for testing,
        creating them at the DB

        """
        
        list = self.genWorkflowObjects(number, name)

        for x in list:
            x.create()

        return list

    def genSubscription(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Subscription Objects for testing,
        creating them at the DB

        """

        list = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        workflow = self.genWorkflow(number = 1, name = name+'Sub')
        fileset = self.genFileset(number = number, name = name+'Sub')

        for i in range(rangemax):        


            subscription = Subscription(fileset = fileset[i], 
                        workflow = workflow[0] )
            subscription.create()

            list.append(subscription)

        return list

    def genJobObjects(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Job Objects for testing

        """

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

        fileset = self.genFileset(number = rangemax, name = name+'Job')
        jobset = []

        for i in range(rangemax):        
         
            job = Job(name = name+'Job'+str(i), files = fileset[i] )
            jobset.append(job)

        return jobset

    def genJob(self, number = 0, name = 'Test'):
        """
        Generate dummy WMBS Job Objects for testing,
        creating them at the DB

        """

        list = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number
        
        joblist = self.genJobObjects(number = rangemax, name = name+'Job')

        subscription = self.genSubscription(number = 1, name = name+'Job')[0]

        #jobset = set(joblist)
        jobgroup = JobGroup(subscription = subscription, jobs = joblist)
        jobgroup.create()

        for job in joblist:                    
            job.create(group = jobgroup)
            list.append(job)

        return list

    def genJobGroup(self, number=0, name='Test'):
        """
        Generate dummy WMBS JobGroup Objects for testing,
        creating them at the DB

        """

        list = []
        jobs = []
        set  = []

        if number == 0:
            rangemax = random.randint(1000, 3000)
        else:
            rangemax = number

            subscription = self.genSubscription(number = 1, 
                            name = name+'JobGroup')[0]            

        for i in range(rangemax):        
            jobs = self.genJobObjects(number = 1, name = name+'JobGroup'+str(i))
            for j in jobs:
                set.append(j)

            jobgroup = JobGroup(subscription = subscription, jobs = set, id = 1)
            list.append(jobgroup)

        return list

    def setUp(self):
        """
        Common setUp for all WMBS Performance tests

        """

        if self._setup:
            return

        self.config_path = 'test.ini'

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.dao = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = self.dao(classname = "Locations.New")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")


        #Total time counter for Performance tests
        self.totaltime = 0

        #verbose definition.  I don't know why it has to be here.
        #self.verbose = "True"
        
        #Number of times each test method will run.
        #Can be overriden at the specific testcases
#        self.testtimes = 10

        #Superclass setUp call
        Performance.setUp(self)

        #Parsing threshold values

        if os.path.exists(self.config_path):
            cfg = ConfigParser()
            
            cfg.read(self.config_path)

            self.verbose   = cfg.get('output', 'verbose')
            self.threshold = float(cfg.get('settings', 'threshold'))
            self.totalthreshold = float(cfg.get('settings', 'total_threshold'))
            self.testtimes = int(cfg.get('settings', 'times'))
        else:
            print "WARNING! Config File (default test.ini) not found.  Using default values"
            self.verbose        = "False"
            self.threshold      = 1
            self.totalthreshold = 5
            self.testtimes      = 1


        self._setup = True
        return

    def tearDown(self):
        """
        Common tearDown for all WMBS Performance tests

        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
        
        myThread.transaction.begin()

        factory = WMFactory("WMBS", "WMCore.WMBS")        
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)

        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        
        myThread.transaction.commit()    
        self._teardown = True
        
        #Post-Testing report        
        if self.totaltime != 0:
            avgtime = self.totaltime/self.testtimes
            
            print 'Elapsed time for %s DAO operations:'\
                    '%.4f seconds (cumulative threshold: %.4f'\
                    % (self.testtimes, self.totaltime, self.totalthreshold)
            print 'Average time for DAO class: %.4f seconds for %s operations'\
                  '(threshold: %.4f)' % (avgtime, self.testtimes, self.threshold)

        #Base tearDown method for the DB Performance test
        Performance.tearDown(self)

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()


    
