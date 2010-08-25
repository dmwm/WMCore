'''
Created on Jul 24, 2009

@author: meloam
'''
import unittest
import os
import threading
import time

from WMComponent.PhEDExInjector.PhEDExInjector import PhEDExInjector
from WMComponent.DBSUpload.DBSUpload import DBSUpload
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList
from WMQuality.TestInit import TestInit
from WMCore.WMException import WMException
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.Agent.Configuration import loadConfigurationFile
import WMComponent.PhEDExInjector.PhEDExInjector
from WMComponent.DBSUpload.DBSUpload import DBSUpload
import WMComponent.DBSUpload.DBSUpload
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException

class TestPhEDExInjector(unittest.TestCase):
    def setUp(self):
        """
        setUP global values
        """
        #dsUrl = "http://cmswttest.cern.ch:7701/phedex/datasvc/xml/tbedi/"
        #dsUrl = "https://cmsweb.cern.ch/phedex/datasvc/xml/tbedi/"
        #self.phedexTestDS = "http://cmswttest.cern.ch/phedex/datasvc/xml/tbedi/"
        self.phedexTestDS = "https://vocms32.cern.ch/phedex/datasvc/json/tbedi/"
        #self.phedexTestDS = "https://localhost:9999/phedex/datasvc/json/tbedi/"
        #To check your authorithy to access
        #https://cmswttest.cern.ch/phedex/datasvc/perl/tbedi/auth?ability=datasvc_subscribe
        
        #self.phedexTestDS = "https://localhost:9999/phedex/datasvc/xml/tbedi/"
        #self.dbsTestUrl = "http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet"
        self.dbsTestUrl = "http://vocms32:8989/DBS/servlet/DBSServlet"
        self.testNode   = "TX_Test1_Buffer"
        self.testNode2  = "TX_Test3_Buffer"
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        # get things right if our databse is in a weird state
        keepGoing = True
        while keepGoing:
            try:
                self.testInit.setSchema(customModules = \
                    ["WMCore.ThreadPool","WMCore.MsgService",\
                      "WMComponent.DBSBuffer.Database"],
                                useDefault = False)
            except WMException, ex:
                if ("already exists" in str(ex)) and\
                   ("INSERT TABLE" in str(ex)):
                    keepGoing = True
                    continue
                else:
                    keepGoing = False
            else:
                keepGoing = False
            
            
        #except WMException, e:
        
       #self.tearDown()
        #aise

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet")
               # read the default config first.
        config = loadConfigurationFile(os.path.join(os.path.dirname(\
                        WMComponent.PhEDExInjector.PhEDExInjector.__file__), 'DefaultConfig.py'))

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "anzar@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Upload"

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
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
            if os.getenv("DATABASE") == 'sqlite://':
                raise RuntimeError,\
                    "These tests will not work using in-memory SQLITE"
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        self.config = config
        
        myThread = threading.currentThread()
        myThread.dbi.processData("""INSERT INTO dbsbuffer_block (blockname, location, is_in_phedex)
                                    VALUES ('block1','100',0)""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_block (blockname, location, is_in_phedex)
                                    VALUES ('block1','101',0)""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_block (blockname, location, is_in_phedex)
                                    VALUES ('block1','102',1)""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_block (blockname, location, is_in_phedex)
                                    VALUES ('block2','100',0)""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_location (id,se_name) VALUES (100,'location1')""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_location (id,se_name) VALUES (101,'location2')""")
        myThread.dbi.processData("""INSERT INTO dbsbuffer_location (id,se_name) VALUES (102,'location3')""")
        
    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        factory2 = WMFactory("MsgService", "WMCore.MsgService")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("Threadpool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        myThread.transaction.commit()
        


    def testDAO(self):
        
        """
        Mimics creation of component and handles come messages.
        """
        
 
        

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 4)
        
        daofactory = DAOFactory(package = "WMComponent.PhEDExInjector.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        getBlocks = daofactory("GetUninjectedBlocks")
        uninjectedBlocks = getBlocks.execute()
        self.assertEqual(len(uninjectedBlocks), 3)
        
        setBlocks = daofactory("SetBlocksInjected")
        blocksSet = setBlocks.execute( uninjectedBlocks )
        uninjectedBlocks = getBlocks.execute()
        self.assertEqual(len(uninjectedBlocks), 0)
        
            
        
#        testPhEDExInject = PhEDExInjector(self.config)
#        testPhEDExInject.prepareToStart()
#        while threading.activeCount() > 1:
#            print('Currently: '+str(threading.activeCount())+\
#                ' Threads. Wait until all our threads have finished')
#            time.sleep(1)



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()