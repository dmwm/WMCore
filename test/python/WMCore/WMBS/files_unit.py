#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: files_unit.py,v 1.2 2008/06/10 17:05:25 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands
from pylint import lint
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction
#pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py

class BaseFilesTestCase(unittest.TestCase):
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
        
        theMySQLCreator = CreateWMBSAction(self.mysqllogger)
        createworked = theMySQLCreator.execute(dbinterface=self.dbf1.connect())
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = CreateWMBSAction(self.sqlitelogger)    
        createworked = theSQLiteCreator.execute(dbinterface=self.dbf2.connect())
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
        
                                              
    def tearDown(self):
        """
        Delete the databases
        """
        self.testlogger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.testlogger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.testlogger.debug("WMBS MySQL database deleted")
        self.testlogger.debug(os.remove('filesettest.lite'))
        self.testlogger.debug("WMBS SQLite database deleted")
        
class CreateListDeleteTestCase(BaseFilesTestCase):
    def setUp(self):
        BaseFilesTestCase.setUp(self)
        
        from WMCore.WMBS.Actions.Fileset.New import NewFilesetAction
        newFS = NewFilesetAction(self.testlogger)
        
        assert newFS.execute(name='fs001', dbinterface=self.dbf1.connect())
        assert newFS.execute(name='fs001', dbinterface=self.dbf2.connect())
        
        from WMCore.WMBS.Actions.Files.New import NewFileAction
        from WMCore.WMBS.Actions.Files.AddToFileset import AddFileToFilesetAction
        from WMCore.WMBS.Actions.Files.InFileset import InFilesetAction
        from WMCore.WMBS.Actions.Files.SetLocation import SetFileLocationAction
        
        self.action1 = NewFileAction(self.testlogger)
        self.action2 = AddFileToFilesetAction(self.testlogger)
        self.action3 = InFilesetAction(self.testlogger)
        self.action4 = SetFileLocationAction(self.testlogger)
        
    def testCreate(self, notest=False):
        file = "/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1231.root"
        
        self.action1.execute(files=file, size=1000, events=2000, run=10, lumi=12312, dbinterface=self.dbf1.connect())
        self.action1.execute(files=file, size=1000, events=2000, run=10, lumi=12312, dbinterface=self.dbf2.connect())
        
        self.action2.execute(file=file, fileset='fs001', dbinterface=self.dbf1.connect())
        self.action2.execute(file=file, fileset='fs001', dbinterface=self.dbf2.connect())
        
        for conn in self.dbf1.connect(), self.dbf2.connect():
            list = self.action3.execute(fileset='fs001', dbinterface=conn)
            assert len(list) == 1, \
                "list has wrong length (%s not 1) \n \t %s" %(len(list), list)
        
    def testListCreate(self, notest=False):
        filelist = []
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1232.root", 1000, 2000, 10, 12312))
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1233.root", 1000, 2000, 10, 12312))
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1234.root", 1000, 2000, 10, 12312))
        
        self.action1.execute(files=filelist, dbinterface=self.dbf1.connect())
        self.action1.execute(files=filelist, dbinterface=self.dbf2.connect())

        def strim(tuple): return tuple[0]
        filelist = map(strim, filelist)
        
        self.action2.execute(file=filelist, fileset='fs001', dbinterface=self.dbf1.connect())
        self.action2.execute(file=filelist, fileset='fs001', dbinterface=self.dbf2.connect())
        
        for conn in self.dbf1.connect(), self.dbf2.connect():
            list = self.action3.execute(fileset='fs001', dbinterface=conn)
            assert len(list) == 3 or notest, \
                "list has wrong length (%s not 3) \n \t %s" %(len(list), list)
    
    def testListDialectNeutral(self):
        self.testListCreate(notest=True)
        mysqllist = self.action3.execute(fileset='fs001', dbinterface=self.dbf1.connect())
        sqllitelist = self.action3.execute(fileset='fs001', dbinterface=self.dbf2.connect())
        
        assert mysqllist == sqllitelist, \
            "Lists are not dialect neutral \n \t %s \n \t %s" % (mysqllist, sqllitelist)
        
    def testLocateFiles(self):
        self.testListCreate(notest=True)
        
        se1 = 'lcgse01.phy.bris.ac.uk'
        se2 = 'lcgse02.phy.bris.ac.uk'
        selist = ['se01.fnal.gov', 'se02.fnal.gov']
        for conn in self.dbf1.connect(), self.dbf2.connect():
            list = self.action3.execute(fileset='fs001', dbinterface=conn)
            self.action4.execute(file=list, sename=se1, dbinterface=conn)
            for i in list:
                self.action4.execute(file=i, sename=se2, dbinterface=conn)
            self.action4.execute(file=list, sename=selist, dbinterface=conn)
            
if __name__ == "__main__":
    unittest.main()