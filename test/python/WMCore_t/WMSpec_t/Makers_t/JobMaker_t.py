#!/usr/bin/env python
"""
_JobMaker Test_

Unittest for JobMaker class

"""


__revision__ = "$Id: JobMaker_t.py,v 1.2 2009/08/26 16:33:33 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import os
import os.path
import unittest
import threading
import tempfile
import logging
import time

import hotshot

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.File         import File
from WMCore.WMBS.Job          import Job
from WMCore.DataStructs.Run   import Run
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Workflow     import Workflow
from WMQuality.TestInit       import TestInit
from WMCore.WMFactory         import WMFactory

from WMCore.WMSpec.Makers.JobMaker import JobMaker
from WMCore.WMSpec.Makers.Interface.CreateWorkArea import CreateWorkArea as testJobMaker

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

class JobMakerTest(unittest.TestCase):
    """
    JobMaker test class

    """
    _setup = False
    _teardown = False
    lock = threading.Condition()

    def setUp(self):

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setSchema(customModules = ["WMCore.MsgService"],
                                useDefault = False)
        self.testInit.setSchema(customModules = ["WMCore.ThreadPool"],
                                useDefault = False)


        self.cwd = os.getcwd()

        myThread = threading.currentThread()

        self._teardown = False

        return


    def tearDown(self):

        myThread = threading.currentThread()
        
        if self._teardown:
            return

        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()

        factory = WMFactory("WMBS", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()


        factory = WMFactory("WMBS", "WMCore.MsgService")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        self._teardown = True


        return


    def createSingleJobGroup(self, nameStr = ''):

        myThread = threading.currentThread()

        myThread.transaction.begin()

        testWorkflow = Workflow(spec = "TestWorkload/TestTask", owner = "Simon",
                                name = "wf001"+nameStr, task="Test")
        testWorkflow.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.create()
        testFileB.create()

        testJobA = Job(name = "TestJobA"+nameStr)
        testJobA.addFile(testFileA)
        
        testJobB = Job(name = "TestJobB"+nameStr)
        testJobB.addFile(testFileB)
        
        testJobGroup.add(testJobA)
        testJobGroup.add(testJobB)

        testJobGroup.commit()

        myThread.transaction.commit()

        return testJobGroup


    def createHugeJobGroup(self, nameStr = ''):
        
        myThread = threading.currentThread()

        myThread.transaction.begin()

        testWorkflow = Workflow(spec = "TestHugeWorkload/TestHugeTask", owner = "mnorman",
                                name = "wf001"+nameStr, task="Test")
        testWorkflow.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        for i in range(0,5000):
            testFile = File(lfn = "/this/is/a/lfn"+nameStr, size = 1024, events = 10)
            testFile.create()
            testJob  = Job(name = 'testJob_%s_%i' %(nameStr, i))
            testJob.addFile(testFile)
            testJobGroup.add(testJob)


        testJobGroup.commit()

        myThread.transaction.commit()

        
        return testJobGroup   

    
    def testASingleJobGroup(self):
        """
        Test for creation of single job groups and threading

        """

        #return

        print "testASingleJobGroup"

        #self._teardown = True

        myThread = threading.currentThread()

        os.chdir('test')

        testJobGroupList = [] 

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMCore/WMSpec/Makers/DefaultConfig.py'))

        config.Agent.contact = "mnorman@fnal.gov"
        config.Agent.teamName = "WMSpec"
        config.Agent.agentName = "JobMaker"

        config.section_("General")
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql'
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT").lower()
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            myThread.database              = os.getenv("DATABASE")
        else:
            print "ERROR: Could not find database setting in environment!"
            print "ABORT: Cannot start without a database"
            raise 'Exception'


	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        myThread.logger = logging.getLogger('DBSBufferTest')
        myThread.dialect = os.getenv("DIALECT")

        options = {}
        if not os.getenv("DBSOCK") == None:
            options['unix_socket'] = os.getenv("DBSOCK")
        dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

        testJobMaker = JobMaker(config)
        testJobMaker.prepareToStart()
        
        dbFactory = DBFactory(myThread.logger, myThread.database, options)
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

        for i in range(0,100):
            testJobGroup = self.createSingleJobGroup(str(i))
            testJobGroupList.append(testJobGroup)

        for job in testJobGroupList:
            if job.exists():
                testJobMaker.handleMessage('MakeJob', {'jobGroupID': job.exists(), 'startDir': os.getcwd()})


        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)


        self.assertEqual(os.path.isdir('TestWorkload'), True)
        self.assertEqual(os.path.isdir('TestWorkload/TestTask'), True)
        self.assertEqual(os.path.isdir('TestWorkload/TestTask/JobCollection_1_0'), True)


        os.chdir(self.cwd)

        os.popen3('rm -r test/TestWorkload/TestTask')
        
        return



    def testMultipleJobGroups(self):
        """
        Test for creation of huge job groups and threading

        """

        print "testMultipleJobGroups"

        #return

        #self._teardown = True

        myThread = threading.currentThread()

        os.chdir('test')

        currDir = os.getcwd()

        testJobGroupList = [] 

        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMCore/WMSpec/Makers/DefaultConfig.py'))

        config.Agent.contact = "mnorman@fnal.gov"
        config.Agent.teamName = "WMSpec"
        config.Agent.agentName = "JobMaker"

        config.section_("General")
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql'
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT").lower()
        #config.CoreDatabase.socket = os.getenv("DBSOCK")
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            myThread.database              = os.getenv("DATABASE")
        else:
            print "ERROR: Could not find database setting in environment!"
            print "ABORT: Cannot start without a database"
            raise 'Exception'


	#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        myThread.logger = logging.getLogger('DBSBufferTest')
        myThread.dialect = os.getenv("DIALECT")

        options = {}
        if not os.getenv("DBSOCK") == None:
            options['unix_socket'] = os.getenv("DBSOCK")
        dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

        testJobMaker = JobMaker(config)
        testJobMaker.prepareToStart()
        
        dbFactory = DBFactory(myThread.logger, myThread.database, options)
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)

        for i in range(0,5):
            testJobGroup = self.createHugeJobGroup(str(i))
            testJobGroupList.append(testJobGroup)

        starttime = time.time()

        for job in testJobGroupList:
            if job.exists():
                testJobMaker.handleMessage('MakeJob', {'jobGroupID': job.exists(), 'startDir': currDir})

        print "The number of threads is %i" %(threading.activeCount())

        #time.sleep(600)

        while (threading.activeCount() > 1):
            print "Waiting for threads to finish"
            time.sleep(1)

        #for job in testJobGroupList:
        #    if job.exists():
        #        self.assertEqual(os.path.isdir(job['uid']), True)

        endtime = time.time()

        print "This process took %i seconds to run" %(int(endtime - starttime))

        self.assertEqual(os.path.isdir('TestHugeWorkload'), True)
        self.assertEqual(os.path.isdir('TestHugeWorkload/TestHugeTask'), True)
        listOfCollections = ['JobCollection_1_0', 'JobCollection_1_1', 'JobCollection_2_0', 'JobCollection_2_1', 'JobCollection_4_0', \
                             'JobCollection_3_0', 'JobCollection_5_0', 'JobCollection_3_1', 'JobCollection_5_1', 'JobCollection_4_1', \
                             'JobCollection_4_2', 'JobCollection_4_3', 'JobCollection_4_4']
        for coll in listOfCollections:
            assert coll in os.listdir('TestHugeWorkload/TestHugeTask'), "Failed to create TestHugeWorkload/TestHugeTask/%s" %(coll)
            self.assertEqual(len(os.listdir('TestHugeWorkload/TestHugeTask/%s' %(coll))), 1000)
        
        #Get rid of all this crap
        os.popen3('rm -r TestHugeWorkload')

        os.chdir(self.cwd)
        
        return

    



if __name__ == "__main__":

    prof = hotshot.Profile("hotshot_stats")
    prof.runcall(unittest.main())
    prof.close()
    #unittest.main() 
