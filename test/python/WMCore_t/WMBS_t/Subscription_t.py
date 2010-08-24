import unittest, os, logging, commands, random
from sets import Set

from WMCore_t.DataStructs_t.Subscription_t import SubscriptionTest as BaseTest
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

class SubscriptionTest(BaseTest):
    def setUp(self):
        "make a logger instance"
        #level=logging.ERROR
        logfile = __file__.replace('.pyc','.log')
        logfile = logfile.replace('.py','.log')
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=logfile,
                    filemode='w')
        
        self.mysqllogger = logging.getLogger('wmbs_mysql')
        self.sqlitelogger = logging.getLogger('wmbs_sqlite')
        self.logger = logging.getLogger('unit_test')
        
        self.tearDown()
        
        self.dbf = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        
        self.daofactory = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf.connect())
        
        createworked = False
        try:
            theMySQLCreator = self.daofactory(classname='CreateWMBS')
            createworked = theMySQLCreator.execute()
        except:
            pass
        if createworked:
            self.logger.debug("WMBS MySQL database created")
        else:
            self.logger.debug("WMBS MySQL database could not be created, already exists?")
           
        self.selist = ['goodse.cern.ch', 'badse.cern.ch', 'averagese.cern.ch']
        try:
            for se in self.selist:
                self.daofactory(classname='Locations.New').execute(sename=se)
        except:
            pass
        #Initial testcase environment
        self.dummyFile = File('/tmp/dummyfile',9999,0,0,0,0, 
                                    logger = self.logger, 
                                    dbfactory = self.dbf)
        self.dummyFile.save()
        self.dummyFile.setLocation('averagese.cern.ch')
        
        self.dummyFileSet = Fileset(name = 'SubscriptionTestFileset', 
                                    logger = self.logger, 
                                    dbfactory = self.dbf)
        self.dummyFileSet.create()
        self.dummyFileSet.addFile(self.dummyFile)
        self.dummyFileSet.commit()
        
        self.dummyWorkFlow = Workflow(spec='spec', owner='me', 
                                      name='testWorkflow', 
                                      logger=self.logger, dbfactory = self.dbf)
        self.dummyWorkFlow.create()
        self.dummySubscription = Subscription(fileset = self.dummyFileSet, 
                                              workflow = self.dummyWorkFlow, 
                                              logger=self.logger, 
                                              dbfactory = self.dbf)
        self.dummySubscription.create()
        
    def tearDown(self):
        """
        Delete the databases
        """
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.logger.debug("WMBS MySQL database deleted")
        try:
            self.logger.debug(os.remove('filesettest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.logger.debug("WMBS SQLite database deleted")
        
    def testFilesOfStatus(self):
        pass
    
    def testFailedFiles(self):
        pass
    
    def testFailFiles(self):
        pass
    
    def testCompletedFiles(self):
        pass
    
    def testCompleteFiles(self):
        pass
    
    def testAcquiredFiles(self):
        pass
    
    def testAcquireFiles(self):
        pass
    
    def testAvailableFiles(self):
        pass
    
    def testAvailableFilesWhiteList(self):
        """
        Testcase for the availableFiles method of the Subscription Class when a 
        white list is present in the subscription.
        """
        count = 0
        fs = self.dummySubscription.getFileset()
        for i in range(1, 100):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi, 
                                    logger = self.logger, 
                                    dbfactory = self.dbf)
            file.save()
            if random.randint(1, 2) > 1:
                file.setLocation('goodse.cern.ch')
                count += 1
            else:
                file.setLocation('badse.cern.ch')
            #Add the new file
            fs.addFile(file)
        fs.commit()    
        self.dummySubscription.markLocation('goodse.cern.ch')
        
        assert count == len(self.dummySubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" %\
        (len(self.dummySubscription.availableFiles()), count)
        
    def testAvailableFilesBlackList(self):
        """
        Testcase for the availableFiles method of the Subscription Class
        """
        count = 0
        fs = self.dummySubscription.getFileset()
        
        for i in range(1, 100):
            lfn = '/blacklist/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi, 
                                    logger = self.logger, 
                                    dbfactory = self.dbf)
            file.save()
            if random.randint(1, 2) > 1:
                file.setLocation('goodse.cern.ch')
            else:
                file.setLocation('badse.cern.ch')
                count += 1
            #Add the new file
            fs.addFile(file)
        fs.commit()   
        
        self.dummySubscription.markLocation('badse.cern.ch', whitelist = False)
        # added 100 files, plus the original one
        assert 100 - count == len(self.dummySubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" %\
        (len(self.dummySubscription.availableFiles()), 100 - count) 
               
    def testAvailableFilesBlackWhiteList(self):
        """
        Testcase for the availableFiles method of the Subscription Class when 
        both a white and black list are provided
        """
        count = 0
        fs = self.dummySubscription.getFileset()
        for i in range(1, 10):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi, 
                                    logger = self.logger, 
                                    dbfactory = self.dbf)
            file.save()
            if random.randint(1, 2) > 1:
                file.setLocation('goodse.cern.ch')
                count += 1
            else:
                file.setLocation('badse.cern.ch')
            #Add the new file
            fs.addFile(file)
        fs.commit()   
        self.dummySubscription.markLocation('badse.cern.ch', whitelist = False)
        self.dummySubscription.markLocation('goodse.cern.ch')
        
        assert count == len(self.dummySubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" %\
        (len(self.dummySubscription.availableFiles()), count)   
        
if __name__ == "__main__":
    unittest.main()        