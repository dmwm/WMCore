#!/usr/bin/env python

import os, unittest, logging, commands, time, random


from unittest import TestCase
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from sets import Set

class Base_t():
    """
    __Base_t__

    Base class for DB Performance at WMBS


    """
    def setUp(self):
        """
        Common setUp for all Performance tests

        """

        #Setting up logger
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBPerformanceTest')

        #Place common execute method arguments here        
        self.baseexec=' '

        self.sename='localhost'        
        
        self.tearDown()
        
        self.DBList = ['MySQL','SQLite']
        mysqlURI = 'mysql://jcg@localhost/wmbs'
#        sqliteURI = 'sqlite:///dbperftest.lite'

        self.mysqldbf = DBFactory(self.logger, mysqlURI)
#        self.sqlitedbf = DBFactory(self.logger, sqliteURI)        

        self.mysqldao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                        dbinterface=self.mysqldbf.connect())
#        self.sqlitedao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
#                        dbinterface=self.sqlitedbf.connect())        
        
        assert self.mysqldao(classname='CreateWMBS').execute()

#        assert self.sqlitedao(classname='CreateWMBS').execute()

        # Create a File to be used as argument for the performance test
        file_lfn = '/tmp/file/fileexample'
        file_events = 1111
        file_size = 1111
        file_run = 111
        file_lumi = 0
        
        #Create File - mySQL
        self.testmysqlFile = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run,
                    lumi=file_lumi, logger=self.logger, dbfactory=self.mysqldbf)
        self.testmysqlFile.save()

        #Create File - SQLite
#        self.testsqliteFile = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run,
#                    lumi=file_lumi, logger=self.logger, dbfactory=self.sqlitedbf)
#        self.testsqliteFile.save()


        # Create a Fileset of random, parentless, childless, unlocatied file
        mysqlfilelist = []
#        sqlitefilelist = []

        #Generating Files - mySQL DBFactory        
        for x in range(random.randint(1000,3000)):
            file = File(lfn='/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                                  random.randint(1000, 9999)),
                        size=random.randint(1000, 2000),
                        events = 1000,
                        run = random.randint(0, 2000),
                        lumi = random.randint(0, 8), 
                        logger=self.logger, 
                        dbfactory=self.mysqldbf)
            
            mysqlfilelist.append(file)

        #Generating Files - SQLite DBFactory        
#        for x in range(random.randint(1000,3000)):
#            file = File(lfn='/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
#                                                  random.randint(1000, 9999)),
#                        size=random.randint(1000, 2000),
#                        events = 1000,
#                        run = random.randint(0, 2000),
#                        lumi = random.randint(0, 8), 
#                        logger=self.logger, 
#                        dbfactory=self.sqlitedbf)
#            
#            sqlitefilelist.append(file)
    
        #Creating mySQL Fileset        
        self.testmysqlFileset = Fileset(name='testFileSet', 
                            files=mysqlfilelist, 
                            logger=self.logger, 
                            dbfactory=self.mysqldbf) 
        self.testmysqlFileset.create()     

        #Creating SQLite Fileset        
#        self.testsqliteFileset = Fileset(name='testFileSet', 
#                            files=sqlitefilelist, 
#                            logger=self.logger, 
#                            dbfactory=self.sqlitedbf) 
#        self.testsqliteFileset.create()     

        #Creating mySQL Workflow
        self.testmysqlWorkflow = Workflow(spec='Test', owner='PerformanceTestCase', name='Test_mysqlWorkflow', logger=self.logger, dbfactory=self.mysqldbf)
        self.testmysqlWorkflow.create()

        #Creating SQLite Workflow
#        self.testsqliteWorkflow = Workflow(spec='Test', owner='PerformanceTestCase', name='Test_mysqlWorkflow', logger=self.logger, dbfactory=self.sqlitedbf)
#        self.testsqliteWorkflow.create()

        #Creating MySQL Subscription
        self.testmysqlSubscription = Subscription(fileset=self.testmysqlFileset, 
                        workflow=self.testmysqlWorkflow, logger=self.logger, 
                        dbfactory=self.mysqldbf)
        self.testmysqlSubscription.create()

        #Creating SQLite Subscription
#        self.testsqliteSubscription = Subscription(fileset=self.testsqliteFileset, 
#                        workflow=self.testsqliteWorkflow, logger=self.logger, 
#                        dbfactory=self.sqlitedbf)
#        self.testsqliteSubscription.create()

        #Instatiating mySQL Job
        self.testmysqlJob = Job(name='TestmysqlJob',files=self.testmysqlFileset, logger=self.logger, dbfactory=self.mysqldbf)

        #Instatiating SQLite Job
#        self.testsqliteJob = Job(name='TestsqliteJob',files=self.testsqliteFileset, logger=self.logger, dbfactory=self.sqlitedbf)

        #Creating mySQL JobGroup
        testsetmysql = Set()
        testsetmysql.add(self.testmysqlJob)
        self.testmysqlJobGroup = JobGroup(subscription=self.testmysqlSubscription, jobs=testsetmysql)

        #Creating SQLite JobGroup
#        testsetsqlite = Set()
#        testsetsqlite.add(self.testsqliteJob)
#        self.testsqliteJobGroup = JobGroup(subscription=self.testsqliteSubscription, jobs=testsetsqlite)

        #Creating mySQL Job for testing
        self.testmysqlJob.create(group=self.testmysqlJobGroup.id)

        #Creating SQLite Job for testing
#        self.testsqliteJob.create(group=self.testsqliteJobGroup.id)

        #Setting the available SEs        
        self.sename='localhost'        

        #Assuring the database is reinitialized        
        self.tearDown()
        
        self.DBList = ['MySQL','SQLite']
        mysqlURI = 'mysql://jcg@localhost/wmbs'
#        sqliteURI = 'sqlite:///dbperftest.lite'

        #Creating the DB Factories and DAOs
        self.mysqldbf = DBFactory(self.logger, mysqlURI)
#        self.sqlitedbf = DBFactory(self.logger, sqliteURI)        

        self.mysqldao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                        dbinterface=self.mysqldbf.connect())
#        self.sqlitedao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
#                        dbinterface=self.sqlitedbf.connect())        
        
        assert self.mysqldao(classname='CreateWMBS').execute()

#        assert self.sqlitedao(classname='CreateWMBS').execute()

        #Creating the Locations at the Database
        self.selist = ['localhost']        
        for se in self.selist:
            self.mysqldao(classname='Locations.New').execute(sename=se)
            #self.sqlitedao(classname='Locations.New').execute(sename=se)      

    def tearDown(self):
        #Base tearDown method for the DB Performance test
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u jcg drop wmbs'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u jcg create wmbs'))
        self.logger.debug("WMBS MySQL database deleted")
        try:
            self.logger.debug(os.remove('dbperftest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.logger.debug("WMBS SQLite database deleted")

    def getClassNames(self, dirname='.'):
        """
        Method that gets the DAO classnames from the directory tree
        Still needs testing, only present here to illustrate the idea
        of make these tests automatic.

        """
        files = os.listdir(dirname)
        list = []
        for x in files:
            #Hack - Only get relevant .py files (no __init__.py, CVS stuff, etc...)
            if (x[-3:] == '.py') and (x[0] != '_'):
                parts = x.split('.')
                list.append(parts[0])

        return list
    
    def perfTest(self, dao, action, execinput=''):
        """
        Method that executes a dao class operation and measures its
        execution time.
        
        """
        
        #Test each of the DAO classes of the specific WMBS class directory        
        startTime = time.time()               
        #Place execute method of the specific classname here            
        dao(classname=action).execute(execinput)
        endTime = time.time()
        diffTime = endTime - startTime
        
        return diffTime
