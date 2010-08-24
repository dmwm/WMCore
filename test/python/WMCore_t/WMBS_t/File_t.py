#!/usr/bin/env python
"""
_File_t_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

Test creates one WMBS database instance which is used for all tests.

"""

__revision__ = "$Id: File_t.py,v 1.1 2008/09/25 13:14:02 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"

import unittest, logging, os, commands
from sets import Set
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from unittest import TestCase
import random

class FileClassTest(TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
            
        mysqllogger = logging.getLogger('wmbs_mysql')
        sqlitelogger = logging.getLogger('wmbs_sqlite')
        
        self.tearDown()
        # Create the databases
        
        self.dbf1 = DBFactory(mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(sqlitelogger, 'sqlite:///filetest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', 
                                      logger=mysqllogger, 
                                      dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', 
                                      logger=sqlitelogger, 
                                      dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        if createworked:
            mysqllogger.debug("WMBS MySQL database created")
        else:
            mysqllogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        if createworked:
            sqlitelogger.debug("WMBS SQLite database created")
        else:
            sqlitelogger.debug("WMBS SQLite database could not be created, already exists?")
        
        self.selist = ['lcgse01.phy.bris.ac.uk', 
                       'lcgse02.phy.bris.ac.uk', 
                       'se01.fnal.gov', 
                       'se02.fnal.gov']
        
        for se in self.selist:
            self.daofactory1(classname='Locations.New').execute(sename=se)
            self.daofactory2(classname='Locations.New').execute(sename=se)  
          
    def tearDown(self):        
        mysqllogger = logging.getLogger('wmbs_mysql')
        sqlitelogger = logging.getLogger('wmbs_sqlite')
        #Delete the databases
    
        mysqllogger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        mysqllogger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        mysqllogger.debug("WMBS MySQL database deleted")
        try:
            sqlitelogger.debug(os.remove('filetest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        sqlitelogger.debug("WMBS SQLite database deleted")
              
    def testInit(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testInit')
        if not dbf:
            dbf=self.dbf1
        # Create a random, parentless, childless, unlocatied file
        lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                              random.randint(1000, 9999))
        size = random.randint(1000, 2000)
        events = 1000
        run = random.randint(0, 2000)
        lumi = random.randint(0, 8)
        
        file = File(lfn=lfn, size=size, events=events, run=run, 
                    lumi=lumi, logger=logger, dbfactory=dbf)
        
        return file, lfn, size, events, run, lumi
        
    def testGetInfo(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testGetInfo')
        if not dbf:
            dbf=self.dbf1
        # Check that a file contains what it should
        file, lfn, size, events, run, lumi = self.testInit(logger, dbf)
        
        info = file.getInfo()
        assert info[0] == lfn, "lfn is wrong"
        assert info[2] == size, "lfn is wrong"
        assert info[3] == events, "lfn is wrong"
        assert info[4] == run, "lfn is wrong"
        assert info[5] == lumi, "lfn is wrong"
        
    def testGetParentLFNs(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testGetParentLFNs')
        if not dbf:
            dbf=self.dbf1
            
        # Check that a file has the correct parents            
        parent1, child = self.testAddParent()
        
        assert child.getParentLFNs()[0] == parent1.dict['lfn'], \
                                                    "child has wrong parent"
                                                    
        parent2, child = self.testAddParent(child=child)
        
        parents = child.getParentLFNs()
        assert len(parents) == 2, "child has %s parents, not 2" % len(parents) 
        assert parent1 in parents and parent2 in parents, \
                                                    "child has wrong parentage"
    
    def testLoad(self, logger=None, dbf=None, lfn=None):
        if not logger:
            logger=logging.getLogger('testLoad')
        if not dbf:
            dbf=self.dbf1
        # Check that a file loads correctly 
        if not lfn:
            lfn = self.testSave(logger)[1]
        file = File(lfn=lfn, logger=logger, dbfactory=dbf)
        file.load()
    
    def testSave(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testSave')
        if not dbf:
            dbf=self.dbf1
        # Check that a file saves correctly
        file, lfn, size, events, run, lumi = self.testInit(logger, dbf)
        file.save()
        return file, lfn, size, events, run, lumi
    
    def testDelete(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testDelete')
        if not dbf:
            dbf=self.dbf1
        # Check that a file deletes correctly
        file, lfn, size, events, run, lumi = self.testSave(logger, dbf)
        file.delete()
        try:
            self.testLoad(logger=logger, lfn=lfn)
        except Exception, e:
            str = e.message
            str = str.lower()
            if str == 'file not found':
                pass
            else:
                raise e

    def testAddChild(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testAddChild')
        if not dbf:
            dbf=self.dbf1
        # Check that children can be added correctly
        # Return the parent and child
        
        child, child_lfn, size, events, run, lumi = self.testSave()
        parent, parent_lfn, size, events, run, lumi = self.testSave()
        
        parent.addChild(child_lfn)
        return parent, child
    
    def testAddParent(self, logger=None, dbf=None, child=None, parent=None):
        if not logger:
            logger=logging.getLogger('testAddParent')
        if not dbf:
            dbf=self.dbf1
        # Check that parents can be added correctly
        # Return the parent and child
        if not child:
            child, child_lfn, size, events, run, lumi = self.testSave()
        if not parent:
            parent, parent_lfn, size, events, run, lumi = self.testSave()
        
        child.addParent(parent_lfn)
        return parent, child
    
    def testSetLocation(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testSetLocation')
        if not dbf:
            dbf=self.dbf1
        # Check that a file is located properly
        file, lfn, size, events, run, lumi = self.testSave(logger, dbf)
        
        assert len(file.dict['locations']) == 0, \
                                        "file has a location before it should"
        file.setLocation(self.selist)
        assert len(file.dict['locations']) == len(self.selist), \
                                        "file has incorrect number of locations"
    
    def testDialectNeutral(self, logger=None, dbf=None):
        if not logger:
            logger=logging.getLogger('testDialectNeutral')
        if not dbf:
            dbf=self.dbf1
        # Test that File object behave the same for MySQL and SQLitepass
        pass
   
     
if __name__ == "__main__":
    unittest.main() 