#!/usr/bin/env python

import logging
import unittest
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory
from nose.plugins.attrib import attr
class LocationTest(unittest.TestCase, WMBSBase):
    __performance__=True
    """
    __LocationTest__

     Performance testcase for WMBS Location class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self):
        #Call common setUp method from WMBSBase
                
        WMBSBase.setUp(self)

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
            startTime = time.time()    
            locationNew = self.dao(classname = "Locations.New")
            for location in locations:
                locationNew.execute(siteName = location)
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'CompleteFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'



    def testList(self, times=1):         
        print "testList"
        
        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        for i in range(times):
            startTime = time.time()    
            locationList = self.dao(classname = "Locations.List")
            currentLocations = locationList.execute()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'List DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'  
                

    def testDelete(self, times=1):         
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        locations = self.genLocation(number=times)

        for i in range(times):
            startTime = time.time()    
            locationDel = self.dao(classname = "Locations.Delete")
            for location in locations:
                locationDel.execute(siteName = location)
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'CompleteFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'


if __name__ == "__main__":
    unittest.main()
