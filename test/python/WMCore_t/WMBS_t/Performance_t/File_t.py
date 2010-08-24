#!/usr/bin/env python

import unittest, time, random

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

class File_t(Base_t,TestCase):
    """
    __File_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1
        # Create a File to be used as argument for the performance test
        file_lfn = '/tmp/file/fileexample'
        file_events = 1111
        file_size = 1111
        file_run = 111
        file_lumi = 0
        self.testFile = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run,
                    lumi=file_lumi, logger=self.logger, dbfactory=self.mysqldbf)
        # Create a Fileset of random, parentless, childless, unlocatied file
        filelist = []
        for x in range(random.randint(1000,3000)):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                                  random.randint(1000, 9999))
            self.size = random.randint(1000, 2000)
            self.events = 1000
            self.run = random.randint(0, 2000)
            self.lumi = random.randint(0, 8)
        
            file = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run, 
                    lumi=file_lumi, logger=self.logger, dbfactory=self.mysqldbf)
            filelist.append(file)
        self.testFileset = Fileset(name='testFileSet', 
                            files=filelist, 
                            logger=self.logger, 
                            dbfactory=self.mysqldbf) 
            

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAdd(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.Add', execinput='files=self.testFileset, size=self.size, events=self.events '+self.baseexec)
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddRunLumi(self): 
        time = self.perfTest(dao=self.mysqldao, action='Files.AddRunLumi', execinput='files=self.testFileset, run=self.run, lumi=self.lumi '+self.baseexec)
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddToFileset(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.AddToFileset', execinput='file=self.testFile, fileset=self.testFileset '+self.baseexec)
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.Delete', execinput='file=self.testFile '+self.baseexec)
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByID(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.GetByID', execinput='files=self.testFileset '+self.baseexec)
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByLFN(self): 
        time = self.perfTest(dao=self.mysqldao, action='Files.GetByLFN', execinput='files=self.testFileset '+self.baseexec)
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetLocation(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.GetLocation', execinput='file=self.testFile '+self.baseexec)
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetParents(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.GetParents', execinput='files=self.testFileset '+self.baseexec)
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

#    def testHeritage(self):
#        self.mysqldao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#        time = self.perfTest(dao=self.mysqldao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.InFileset', execinput='files=self.testFileset '+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testSetLocation(self):
        time = self.perfTest(dao=self.mysqldao, action='Files.SetLocation', execinput='file=self.testFile, sename=self.sename '+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()
