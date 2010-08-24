#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: files_DAOFactory_t.py,v 1.6 2008/12/26 15:31:19 afaq Exp $"
__version__ = "$Revision: 1.6 $"

import unittest, logging, os, commands
from sets import Set
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

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
        
        self.tearDown()
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
        
        self.selist = ['lcgse01.phy.bris.ac.uk', 'lcgse02.phy.bris.ac.uk', 'se01.fnal.gov', 'se02.fnal.gov']
        
        for se in self.selist:
            self.daofactory1(classname='Locations.New').execute(sename=se)
            self.daofactory2(classname='Locations.New').execute(sename=se)
                                              
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
        
class FilesDAOObjectTestCase(BaseFilesTestCase):
    def setUp(self):
        BaseFilesTestCase.setUp(self)
        
        newFS = self.daofactory1(classname='Fileset.New')
        assert newFS.execute(name='fs001')
        
        newFS = self.daofactory2(classname='Fileset.New')
        assert newFS.execute(name='fs001')
        
        self.action1 = self.daofactory1(classname='Files.Add')
        self.action5 = self.daofactory1(classname='Files.AddRunLumi')
        self.action2 = self.daofactory1(classname='Files.AddToFileset')
        self.action3 = self.daofactory1(classname='Files.InFileset')
        self.action4 = self.daofactory1(classname='Files.SetLocation')
        
        self.action1a = self.daofactory2(classname='Files.Add')
        self.action5a = self.daofactory2(classname='Files.AddRunLumi')
        self.action2a = self.daofactory2(classname='Files.AddToFileset')
        self.action3a = self.daofactory2(classname='Files.InFileset')
        self.action4a = self.daofactory2(classname='Files.SetLocation')
        
    def testCreate(self, notest=False):
        file = "/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1231.root"
        
        self.action1.execute(files=file, size=1000, events=2000)
        self.action1a.execute(files=file, size=1000, events=2000)
        self.action5.execute(files=file, run=10, lumi=12312)
        self.action5a.execute(files=file, run=10, lumi=12312)
        
        self.action2.execute(file=file, fileset='fs001')
        self.action2a.execute(file=file, fileset='fs001')
        
        list = self.action3.execute(fileset='fs001')
        assert len(list) == 1, \
            "action3 list has wrong length (%s not 1) \n \t %s" %(len(list), list)
        
        list = self.action3a.execute(fileset='fs001')
        assert len(list) == 1, \
            "action3a list has wrong length (%s not 1) \n \t %s" %(len(list), list)
        
    def testListCreate(self, notest=False):
        filelist = []
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1232.root", 1000, 2000, 10, 12312))
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1233.root", 1000, 2000, 10, 12312))
        filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1234.root", 1000, 2000, 10, 12312))
        
        self.action1.execute(files=filelist)
        self.action1a.execute(files=filelist)
        self.action5.execute(files=filelist)
        self.action5a.execute(files=filelist)

        def strim(tuple): return tuple[0]
        filelist = map(strim, filelist)
        
        self.action2.execute(file=filelist, fileset='fs001')
        self.action2a.execute(file=filelist, fileset='fs001')
        
        list = self.action3.execute(fileset='fs001')
        assert len(list) == 3 or notest, \
            "list has wrong length (%s not 3) \n \t %s" %(len(list), list)
            
        list = self.action3a.execute(fileset='fs001')
        assert len(list) == 3 or notest, \
            "list has wrong length (%s not 3) \n \t %s" %(len(list), list)
    
    def testListDialectNeutral(self):
        self.testListCreate(notest=True)
        mysqllist = self.action3.execute(fileset='fs001')
        sqllitelist = self.action3a.execute(fileset='fs001')
        
        assert mysqllist == sqllitelist, \
            "Lists are not dialect neutral \n \t %s \n \t %s" % (mysqllist, sqllitelist)
        
    def testLocateFiles(self):
        self.testListCreate(notest=True)
                
        list = self.action3.execute(fileset='fs001')
        lista = self.action3a.execute(fileset='fs001')
        
        def strim(tuple): return tuple[1]
        
        list = map(strim, list)
        lista = map(strim, lista)
        
        self.action4.execute(file=list, sename=self.selist[0])
        self.action4a.execute(file=lista, sename=self.selist[0])
        
        for i in list:
            self.action4.execute(file=i, sename=self.selist[1])
            
        for i in lista:    
            self.action4a.execute(file=i, sename=self.selist[1])
            
        self.action4.execute(file=list, sename=self.selist[2:])
        self.action4a.execute(file=list, sename=self.selist[2:])
    
class FileBusinessObjectTestCase(BaseFilesTestCase):
    def setUp(self):
        BaseFilesTestCase.setUp(self)
        
    def testFile(self):
        file = "/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1231.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        myfile1 = File(lfn=file, logger=self.mysqllogger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=self.sqlitelogger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
        
        assert myfile1 == myfile2, "Files not the same"
        
        file = "/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1645.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=2000, events=1000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile3 = File(lfn=file, logger=self.mysqllogger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=self.sqlitelogger, dbfactory=self.dbf2)
        
        assert myfile1 != myfile3, "Different files are equal!"
        assert myfile2 != myfile4, "Different files are equal!"
    
    def testFileParent(self):
        logger = logging.getLogger('FileParents_unit_test')
        parentlfn = "/store/data/Electrons/1234/5678/parent.toor"
        childlfn = "/store/data/Electrons/1234/5678/child.toor"
        for dbf in self.dbf1, self.dbf2:
            parent = File(lfn=parentlfn, logger=logger, dbfactory=dbf)
            child = File(lfn=childlfn, logger=logger, dbfactory=dbf)
            parent.save()
            child.save()
            parent.load()
            child.load()
            
            parents = Set()
            parents.add(parent)
            child.addParent(parentlfn)
            #print "child.parents %s" % child.parents
            #print "parents %s" % parents
              
            assert child.dict['parents'] == parents, "Parents do not match"
            
            child.load(1)
            
            assert child.dict['parents'] == parents, "Parents do not match"
            
            child.delete()
            parent.delete()
            
    def testFileLocation(self):
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1231.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
                
        myfile1.setLocation(self.selist[0])
        myfile2.setLocation(self.selist[0]) 
        
        assert len(myfile1.dict['locations']) == 1
        assert len(myfile2.dict['locations']) == 1
           
        myfile1.setLocation(self.selist[1:])
        myfile2.setLocation(self.selist[1:]) 

        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] == myfile3.dict['locations']
        assert myfile2.dict['locations'] == myfile4.dict['locations']
    
    def testFileLocation2(self):
        """
        Tests delayed addition of file locations - ensures can use
        updateLocations method
        """
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/2345/3456/asdfljkeoivjlk2394587.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
                
        myfile1.setLocation(self.selist[0], immediateSave=False)
        myfile2.setLocation(self.selist[0], immediateSave=False) 
        
        assert len(myfile1.dict['locations']) == 1
        assert len(myfile2.dict['locations']) == 1
           
        myfile1.setLocation(self.selist[1:], immediateSave=False)
        myfile2.setLocation(self.selist[1:], immediateSave=False) 
        
        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        # Now perform the delayed update
        myfile1.updateLocations()
        myfile2.updateLocations()
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] == myfile3.dict['locations']
        assert myfile2.dict['locations'] == myfile4.dict['locations']
    
    def testFileLocation3(self):
        """
        Tests delayed addition of file locations - ensures can use save method
        """
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/4567/2345/jfghjdrth45y45s5s.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
                
        myfile1.setLocation(self.selist[0], immediateSave=False)
        myfile2.setLocation(self.selist[0], immediateSave=False) 
        
        assert len(myfile1.dict['locations']) == 1
        assert len(myfile2.dict['locations']) == 1
           
        myfile1.setLocation(self.selist[1:], immediateSave=False)
        myfile2.setLocation(self.selist[1:], immediateSave=False) 
        
        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        # Now perform the delayed update
        myfile1.save()
        myfile2.save()
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] == myfile3.dict['locations']
        assert myfile2.dict['locations'] == myfile4.dict['locations']
    
    def testFileLocation4(self):
        """
        Tests delayed addition of file locations - ensures changes are not
        persisted unless explicitly saved
        """
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/2222/3333/5etyes5te5te5te5t.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
                
        myfile1.setLocation(self.selist[0], immediateSave=False)
        myfile2.setLocation(self.selist[0], immediateSave=False) 
        
        assert len(myfile1.dict['locations']) == 1
        assert len(myfile2.dict['locations']) == 1
           
        myfile1.setLocation(self.selist[1:], immediateSave=False)
        myfile2.setLocation(self.selist[1:], immediateSave=False) 
        
        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] != myfile3.dict['locations']
        assert myfile2.dict['locations'] != myfile4.dict['locations']
    
    def testFileLocation5(self):
        """
        Ensures Sets can be passed as se arguments to a file location. Tests
        both set and sets.Set
        """
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/8462/1636/e456dsthhs565sdfsdg.root"
        
        self.daofactory1(classname='Files.Add').execute(files=file, size=1000, events=2000)
        self.daofactory2(classname='Files.Add').execute(files=file, size=1000, events=2000)
        #self.daofactory1(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        #self.daofactory2(classname='Files.AddRunLumi').execute(files=file, run=10, lumi=12312)
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile1.load()
        myfile2.load()
                
        myfile1.setLocation(set([self.selist[0]]))
        myfile2.setLocation(set([self.selist[0]]))
        
        assert len(myfile1.dict['locations']) == 1, \
                "Expected 1 location, found %d" % len(myfile1.dict['locations'])
        assert len(myfile2.dict['locations']) == 1, \
                "Expected 1 location, found %d" % len(myfile2.dict['locations'])
           
        myfile1.setLocation(Set(self.selist[1:]))
        myfile2.setLocation(Set(self.selist[1:])) 
        
        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] == myfile3.dict['locations']
        assert myfile2.dict['locations'] == myfile4.dict['locations']
    
    def testFileLocation6(self):
        """
        Ensures locations can be added before file is saved with delayed
        persistence
        """
        logger = logging.getLogger('FileLocation_unit_test')
         
        file = "/store/data/Electrons/8236/1379/rrrrasgdflj4eii.root"
        
        myfile1 = File(lfn=file, logger=logger, dbfactory=self.dbf1, size=1000,
                       events=2000, cksum=1)
        myfile1.addRun(Run(10, *[12312]))

        myfile2 = File(lfn=file, logger=logger, dbfactory=self.dbf2, size=1000,
                       events=2000, cksum=1)
        myfile2.addRun(Run(10, *[12312]))
                
        myfile1.setLocation(self.selist[0], immediateSave=False)
        myfile2.setLocation(self.selist[0], immediateSave=False)
        
        assert len(myfile1.dict['locations']) == 1, \
                "Expected 1 location, found %d" % len(myfile1.dict['locations'])
        assert len(myfile2.dict['locations']) == 1, \
                "Expected 1 location, found %d" % len(myfile2.dict['locations'])
           
        myfile1.setLocation(self.selist[1:], immediateSave=False)
        myfile2.setLocation(self.selist[1:], immediateSave=False) 
        
        assert myfile1.dict['locations'] == Set(self.selist)
        assert myfile2.dict['locations'] == Set(self.selist)
        
        myfile1.save()
        myfile2.save()
        
        # Check that the persistency is correct
        myfile3 = File(lfn=file, logger=logger, dbfactory=self.dbf1)
        myfile4 = File(lfn=file, logger=logger, dbfactory=self.dbf2)
        
        myfile3.load()
        myfile4.load()
        
        assert myfile1.dict['locations'] == myfile3.dict['locations']
        assert myfile2.dict['locations'] == myfile4.dict['locations']
        
if __name__ == "__main__":
    unittest.main()
