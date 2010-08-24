#!/usr/bin/env python
"""
_SetupTestCase_

Unit tests for creating WMBS databases and adding in locations, 
including checks to see that calls are database dialect neutral

"""

__revision__ = "$Id: setup_unit.py,v 1.2 2008/06/10 16:51:14 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands
from pylint import lint
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction

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
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
    
    def testSchemaCreation(self):
        self.tearDown() #Guarantee a clean slate - seems a test isn't tearing down properly...
        theMySQLCreator = CreateWMBSAction(self.mysqllogger)
        createworked = theMySQLCreator.execute(dbinterface=self.dbf1.connect())
        
        assert createworked, \
            self.mysqllogger.exception("Creating Schema Failed!!")
        
        theSQLiteCreator = CreateWMBSAction(self.sqlitelogger)    
        createworked = theSQLiteCreator.execute(dbinterface=self.dbf2.connect())
        
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
        
        theMySQLCreator = CreateWMBSAction(self.mysqllogger)
        theMySQLCreator.execute(dbinterface=self.dbf1.connect())
        theSQLiteCreator = CreateWMBSAction(self.sqlitelogger)    
        theSQLiteCreator.execute(dbinterface=self.dbf2.connect())
        
        from WMCore.WMBS.Actions.Locations.New import NewLocationAction
        from WMCore.WMBS.Actions.Locations.List import ListLocationsAction
        from WMCore.WMBS.Actions.Locations.Delete import DeleteLocationAction
        self.action1 = NewLocationAction(self.testlogger)
        self.action2 = ListLocationsAction(self.testlogger)
        self.action3 = DeleteLocationAction(self.testlogger)
        
    def testAddLocation(self, size = 10):
        for d in self.dbf1.connect(), self.dbf2.connect():
            for i in range(size):
                se = "lcgse%s.phy.bris.ac.uk" % i
                self.action1.execute(sename=se, dbinterface=d)
            
    def testListLocations(self):
        size = 50
        self.testAddLocation(size)
        
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
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
        self.action1.execute(sename=se, dbinterface=self.dbf1.connect())
        self.action1.execute(sename=se, dbinterface=self.dbf2.connect())
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
        
        #Check we are starting off right and record size
        assert len(themysqllist) == len(thesqllitelist)
        sizemysql = len(themysqllist)
        sizesqlite = len(thesqllitelist)
        
        #Delete the SE
        self.action3.execute(sename=se, dbinterface=self.dbf1.connect())
        self.action3.execute(sename=se, dbinterface=self.dbf2.connect())
        
        #Run the checks
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == sizemysql - 1
        assert len(thesqllitelist) == sizesqlite - 1
        
        def strim(tuple): return tuple[1]
        themysqllist = map(strim, themysqllist)
        thesqllitelist = map(strim, thesqllitelist)

        self.action3.execute(sename=themysqllist, dbinterface=self.dbf1.connect())
        self.action3.execute(sename=thesqllitelist, dbinterface=self.dbf2.connect())

        #Run the checks
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == 0
        assert len(thesqllitelist) == 0
    
    def testListAsInput(self):
        selist = []
        size = 100
        for i in range(size):
            se = "lcgse%s.phy.bris.ac.uk" % i
            selist.append(se)
        
        self.action1.execute(sename=selist, dbinterface=self.dbf1.connect())
        self.action1.execute(sename=selist, dbinterface=self.dbf2.connect())
            
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == len(selist)
        assert len(thesqllitelist) == len(selist)
        
        self.action3.execute(sename=selist, dbinterface=self.dbf1.connect())
        self.action3.execute(sename=selist, dbinterface=self.dbf2.connect())

        #Run the checks
        themysqllist = self.action2.execute(dbinterface=self.dbf1.connect())
        thesqllitelist = self.action2.execute(dbinterface=self.dbf2.connect())
        assert len(themysqllist) == len(thesqllitelist)
        assert len(themysqllist) == 0
        assert len(thesqllitelist) == 0
        
if __name__ == "__main__":
    unittest.main()            