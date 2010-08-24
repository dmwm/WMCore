#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

class JobGroupTest(WMBSBase):
    """
    __JobGroupTest__

     Performance testcase for WMBS JobGroup class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'JobGroupPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

        #Type the number of times you want the tests to be run
#        self.testtimes = 0

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testNew(self, times=1):         
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        subscription=self.genSubscription(number=times, name='testNew')

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='JobGroup.New', subscription=subscription[i]['id'])
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'        

#    def testLoad(self):   
        # Still no complete JobGroup.Load class
#        print "testLoad"

#        for i in range(times):             
#            time = self.perfTest(dao=self.dao, action='Jobs.Load')
#            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testStatus(self, times=1):         
        print "testStatus"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobgroup=self.genJobGroup(number=1, name='testStatus')[0]

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='JobGroup.Status', group=jobgroup.id)
            assert self.totaltime <= self.totalthreshold, 'Status DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
