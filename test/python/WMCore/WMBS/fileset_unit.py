#!/usr/bin/env python
"""
_FilesetExistsTestCase_

Unit tests for Fileset creation and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: fileset_unit.py,v 1.5 2008/06/10 17:21:03 metson Exp $"
__version__ = "$Revision: 1.5 $"

import unittest, logging, os, commands
from pylint import lint
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Actions.CreateWMBS import CreateWMBSAction
#pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py

class BaseFilesetTestCase(unittest.TestCase):
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

class FilesetExistsTestCase(BaseFilesetTestCase):
    def setUp(self):
        BaseFilesetTestCase.setUp(self)
        
        from WMCore.WMBS.Actions.Fileset.Exists import FilesetExistsAction
        from WMCore.WMBS.Actions.Fileset.New import NewFilesetAction
        from WMCore.WMBS.Actions.Fileset.Delete import DeleteFilesetAction
        from WMCore.WMBS.Actions.Fileset.List import ListFilesetAction
        from WMCore.WMBS.Actions.Fileset.AddAndList import AddAndListFilesetAction
        
        self.action1 = FilesetExistsAction(self.testlogger)
        self.action2 = NewFilesetAction(self.testlogger) 
        self.action3 = DeleteFilesetAction(self.testlogger)  
        self.action4 = ListFilesetAction(self.testlogger)
        self.action5 = AddAndListFilesetAction(self.testlogger)
                
    def testCreateExists(self):
        for conn in self.dbf1.connect(), self.dbf2.connect():
            assert self.action1.execute(name='fs001',
                               dbinterface=conn) == False, \
                               'workflow exists before it has been created'

            assert self.action2.execute(name='fs001',
                               dbinterface=conn) == True, \
                               'workflow cannot be created'

            assert self.action1.execute(name='fs001',
                               dbinterface=conn) == True, \
                               'workflow does not exist after it has been created'   
                               
            assert self.action3.execute(name='fs001',
                               dbinterface=conn) == True, \
                               'workflow cannot be deleted'
                               
            assert self.action1.execute(name='fs001',
                               dbinterface=conn) == False, \
                               'workflow exists after it has been deleted'
                               
        print " Create/Exist actions work as expected for MySQL and SQLite"

    def insertFilesets(self, size=5, conn=None):
        type(conn)
        for i in range(size):
            try:
                self.action2.execute(name='fs00%s' % i, dbinterface=conn)
            except Exception, e:
                print e
    
    def testListDialectNeutral(self):
        c1 = self.dbf1.connect()
        c2 = self.dbf2.connect()
        s = 5
        self.insertFilesets(conn=c1, size=s)  
        self.insertFilesets(conn=c2, size=s)  
        themysqllist = self.action4.execute(dbinterface=c1)
        thesqllitelist = self.action4.execute(dbinterface=c2)
        self.testlogger.debug(themysqllist)
        self.testlogger.debug(thesqllitelist)        
                
        for i in themysqllist, thesqllitelist:
            assert len(i) == s, \
                'lists is wrong size %s not %s\n \t %s' % (len(i), s, i)
            assert type(i) == type([]), \
                'lists is not type list \n \t %s' % (type(i))
        
        assert themysqllist == thesqllitelist, \
            'lists do not match \n \t %s \n \t %s' % (themysqllist, thesqllitelist)
            
        print " List action is dialect neutral" 

    def testAddAndList(self):
        for conn in self.dbf1.connect(), self.dbf2.connect():
            result = self.action5.execute(fileset='fs001', dbinterface=conn)
            assert type(result) == type([]), 'AddAndListFilesetAction did not return a list'
            assert len(result) == 1,\
                'List from AddAndListFilesetAction is of unexpected length (%s not 1) \n\t %s' % (len(result), result)
                
        print " AddAndListFilesetAction works as expected"
if __name__ == "__main__":
    unittest.main()