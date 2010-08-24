#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class File_t(Base_t,TestCase):
    """
    __File_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAdd(self):
        print "testAdd"

        time = self.perfTest(dao=self.mysqldao, action='Files.Add', execinput=['files=self.testmysqlFile["lfn"]', 'size=self.testmysqlFile["size"]', 'events=self.testmysqlFile["events"]'])
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddRunLumi(self): 
        print "testAddRunLumi"

        time = self.perfTest(dao=self.mysqldao, action='Files.AddRunLumi', execinput=['files=self.testmysqlFileset.name', 'run=self.run', 'lumi=self.lumi'])
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddToFileset(self):
        print "testAddToFileset"

        time = self.perfTest(dao=self.mysqldao, action='Files.AddToFileset', execinput=['file=self.testmysqlFile.lfn', 'fileset=self.testmysqlFileset.name '])
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):
        print "testDelete"

        time = self.perfTest(dao=self.mysqldao, action='Files.Delete', execinput=['file=self.testmysqlFile'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByID(self):
        print "testGetByID"

        time = self.perfTest(dao=self.mysqldao, action='Files.GetByID', execinput=['files=self.testmysqlFile.id'])
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByLFN(self):
        print "testGetByLFN"

        time = self.perfTest(dao=self.mysqldao, action='Files.GetByLFN', execinput=['files=self.testmysqlFile.lfn'])
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetLocation(self):
        print "testGetLocation"

        time = self.perfTest(dao=self.mysqldao, action='Files.GetLocation', execinput=['files=self.testmysqlFileset'])
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetParents(self):
        print "testGetParents"
        
        time = self.perfTest(dao=self.mysqldao, action='Files.GetParents', execinput=['files=self.testmysqlFileset'])
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

#    def testHeritage(self):
#        self.mysqldao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#        time = self.perfTest(dao=self.mysqldao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self):
        print "testInFileset"

        time = self.perfTest(dao=self.mysqldao, action='Files.InFileset', execinput=['files=self.testmysqlFileset'])#+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testSetLocation(self):
        print "testSetLocation"

        time = self.perfTest(dao=self.mysqldao, action='Files.SetLocation', execinput=['file=self.testmysqlFile', 'sename=self.sename'])#+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()
