#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

class LocationTest(WMBSBase):
    """
    __LocationTest__

     Performance testcase for WMBS Location class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'FilePerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testNew(self):         
        print "testNew"
        
        sename='TestLocation'

        time = self.perfTest(dao=self.dao, action='Locations.New', sename=sename)
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testList(self):         
        print "testList"
        
        sename='TestLocation'

        time = self.perfTest(dao=self.dao, action='Locations.List')
        assert time <= self.threshold, 'List DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):         
        print "testDelete"
        
        time = self.perfTest(dao=self.dao, action='Locations.Delete', sename=self.sename)
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

#    def testFiles(self):         
#        print "testFiles"
#        
#        time = self.perfTest(dao=self.mysqldao, action='Locations.Files', execinput=['sename="TestLocation"'])
#        assert time <= self.threshold, 'Files DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'
