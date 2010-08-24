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

    def testNew(self, times=1):         
        print "testNew"
        
        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        locations = self.genLocationObjects(number=times)

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Locations.New', sename=locations[i])
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'   

    def testList(self, times=1):         
        print "testList"
        
        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Locations.List')
            assert self.totaltime <= self.totalthreshold, 'List DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'   

    def testDelete(self, times=1):         
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        locations = self.genLocation(number=times)

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Locations.Delete', sename=locations[i])
            assert self.totaltime <= self.totalthreshold, 'Delete DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'   


#    def testFiles(self):         
#        print "testFiles"
#        
#        time = self.perfTest(dao=self.mysqldao, action='Locations.Files', execinput=['sename="TestLocation"'])
#        assert time <= self.threshold, 'Files DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'
