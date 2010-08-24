#!/usr/bin/env python
"""
_SetupTestCase_

Unit tests for creating WMBS databases and adding in locations, 
including checks to see that calls are database dialect neutral

"""

__revision__ = "$Id: setup_DAOFactory_unit.py,v 1.2 2008/06/24 11:45:23 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory

class BaseSetupTestCase(unittest.TestCase):
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
        
        self.tearDown() #Guarantee a clean slate - seems a test isn't tearing down properly...
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        
    def testSchemaCreation(self):
        self.tearDown() #Guarantee a clean slate - seems a test isn't tearing down properly...
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        
        assert createworked, \
            self.mysqllogger.exception("Creating Schema Failed!!")
                
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        self.testlogger.debug('createworked: %s' % createworked)
        assert createworked, \
            self.sqlitelogger.exception("Creating Schema Failed!!")

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
        
class AddLocationsTestCase(BaseSetupTestCase):
    def setUp(self):
        BaseSetupTestCase.setUp(self)
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        theMySQLCreator.execute()
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')  
        theSQLiteCreator.execute()
        
        i = 0
        self.action1 = []
        self.action2 = []
        self.action3 = []
        for daofactory in self.daofactory1, self.daofactory2:
            self.action1.append(daofactory(classname='Locations.New'))
            self.action2.append(daofactory(classname='Locations.List'))
            self.action3.append(daofactory(classname='Locations.Delete'))
        
    def testAddLocation(self, size = 10):
        for d in 0,1:
            for i in range(size):
                se = "lcgse%s.phy.bris.ac.uk" % i
                self.action1[d].execute(sename=se)
            
    def testListLocations(self):
        size = 50
        self.testAddLocation(size)
        
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        self.testlogger.debug(themysqllist)
        self.testlogger.debug(thesqllitelist)        
                
        for i in themysqllist, thesqllitelist:
            assert len(i) == size, \
                'lists do not match \n \t %s \n \t %s' % (themysqllist, thesqllitelist)
            assert type(i) == type([]), \
                'lists do not match \n \t %s \n \t %s' % (themysqllist, thesqllitelist)
        
        assert themysqllist == thesqllitelist, \
            'lists do not match \n \t %s \n \t %s' % (themysqllist, thesqllitelist)
            
        print " List action is dialect neutral"     
            
    def testDeleteLocations(self):
        #Make a list of locations
        size = 10
        self.testAddLocation(size)
        
        #Add in an SE to be deleted
        se = 'setodelete.cern.ch'
        self.action1[0].execute(sename=se)
        self.action1[1].execute(sename=se)
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        
        #Check we are starting off right and record size
        assert len(themysqllist) == len(thesqllitelist)
        sizemysql = len(themysqllist)
        sizesqlite = len(thesqllitelist)
        
        #Delete the SE
        self.action3[0].execute(sename=se)
        self.action3[1].execute(sename=se)
        
        #Run the checks
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == sizemysql - 1
        assert len(thesqllitelist) == sizesqlite - 1
        
        def strim(tuple): return tuple[1]
        themysqllist = map(strim, themysqllist)
        thesqllitelist = map(strim, thesqllitelist)

        self.action3[0].execute(sename=themysqllist)
        self.action3[1].execute(sename=thesqllitelist)

        #Run the checks
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == 0
        assert len(thesqllitelist) == 0
    
    def testListAsInput(self):
        selist = []
        size = 100
        for i in range(size):
            se = "lcgse%s.phy.bris.ac.uk" % i
            selist.append(se)
        
        self.action1[0].execute(sename=selist)
        self.action1[1].execute(sename=selist)
            
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == len(selist)
        assert len(thesqllitelist) == len(selist)
        
        self.action3[0].execute(sename=selist)
        self.action3[1].execute(sename=selist)

        #Run the checks
        themysqllist = self.action2[0].execute()
        thesqllitelist = self.action2[1].execute()
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == 0
        assert len(thesqllitelist) == 0
        
if __name__ == "__main__":
    unittest.main()            