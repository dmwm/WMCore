#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class File_t(Base_t):
    """
    __File_t__

     Performance testcase for WMBS File class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'FilePerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAdd(self):
        print "testAdd"
        
        time = self.perfTest(dao=self.dao, action='Files.Add', execinput=['files=self.testFile["lfn"]', 'size=self.testFile["size"]', 'events=self.testFile["events"]'])
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddRunLumi(self): 
        print "testAddRunLumi"
                
        time = self.perfTest(dao=self.dao, action='Files.AddRunLumi', execinput=['files=self.testFileset.name', 'run=self.run', 'lumi=self.lumi'])
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddToFileset(self):
        print "testAddToFileset"
        
        time = self.perfTest(dao=self.dao, action='Files.AddToFileset', execinput=['file=self.testFile.lfn', 'fileset=self.testFileset.name '])
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):
        print "testDelete"
        
        time = self.perfTest(dao=self.dao, action='Files.Delete', execinput=['file=self.testFile'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByID(self):
        print "testGetByID"
        
        time = self.perfTest(dao=self.dao, action='Files.GetByID', execinput=['files=self.testFile.id'])
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByLFN(self):
        print "testGetByLFN"
         
        time = self.perfTest(dao=self.dao, action='Files.GetByLFN', execinput=['files=self.testFile.lfn'])
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetLocation(self):
        print "testGetLocation"
        
        time = self.perfTest(dao=self.dao, action='Files.GetLocation', execinput=['files=self.testFileset'])
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetParents(self):
        print "testGetParents"
                
        time = self.perfTest(dao=self.dao, action='Files.GetParents', execinput=['files=self.testFileset'])
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


#    def testHeritage(self):
#        self.dao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#        time = self.perfTest(dao=self.dao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self):
        print "testInFileset"
        
        time = self.perfTest(dao=self.dao, action='Files.InFileset', execinput=['files=self.testFileset'])#+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testSetLocation(self):
        print "testSetLocation"

        time = self.perfTest(dao=self.dao, action='Files.SetLocation', execinput=['file=self.testFile', 'sename=self.sename'])#+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


if __name__ == "__main__":
    unittest.main()
