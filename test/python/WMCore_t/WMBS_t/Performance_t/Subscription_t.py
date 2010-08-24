#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

class SubscriptionTest(WMBSBase):
    """
    __SubscriptionTest__

     Performance testcase for Subscription DAO class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'SubscriptionPerformanceTest')
        
        self.totaltime = 0
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testAcquireFiles(self, times=1):         
        print "testAcquireFiles"

        subscription = self.genSubscription(number=1)[0]
       
        filelist = self.genFiles(number=times)

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Subscriptions.AcquireFiles', subscription=subscription["id"],file=filelist[i]["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'AcquireFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testCompleteFiles(self, times=1):         
        print "testCompleteFiles"

        subscription = self.genSubscription(number=1)[0]
       
        filelist = self.genFiles(number=times)

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Subscriptions.CompleteFiles', subscription=subscription["id"],file=filelist[i]["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'CompleteFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'   

    def testFailFiles(self, times=1):         
        print "testFailFiles"

        subscription = self.genSubscription(number=1)[0]
       
        filelist = self.genFiles(number=times)

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Subscriptions.FailFiles', subscription=subscription["id"],file=filelist[i]["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'FailFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testDeleteAcquiredFiles(self, times=1):         
        print "testDeleteAcquiredFiles"

        subscription = self.genSubscription(number=1)[0]
       
        filelist = self.genFiles(number=times)

        for i in range(times):     

            self.dao(classname='Subscriptions.AcquireFiles').execute(subscription=subscription["id"], file=filelist[i]["id"])

            time = self.perfTest(dao=self.dao, action='Subscriptions.DeleteAcquiredFiles', subscription=subscription["id"],file=filelist[i]["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'DeleteAcquiredFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetAcquiredFiles(self, times=1):         
        print "testGetAcquiredFiles"

        subscription = self.genSubscription(number=1)[0]

        filelist = self.genFiles(number=times)

        for i in range(times):     

            self.dao(classname='Subscriptions.AcquireFiles').execute(subscription=subscription["id"], file=filelist[i]["id"])

            time = self.perfTest(dao=self.dao, action='Subscriptions.GetAcquiredFiles', subscription=subscription["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'GetAcquiredFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetAvailableFiles(self, times=1):         
        print "testGetAvailableFiles"

        subscription = self.genSubscription(number=1)[0]

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Subscriptions.GetAvailableFiles', subscription=subscription["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'GetAvailableFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetCompletedFiles(self, times=1):         
        print "testGetCompletedFiles"

        subscription = self.genSubscription(number=1)[0]

        filelist = self.genFiles(number=times)

        for i in range(times):     

            self.dao(classname='Subscriptions.CompleteFiles').execute(subscription=subscription["id"], file=filelist[i]["id"])

            time = self.perfTest(dao=self.dao, action='Subscriptions.GetCompletedFiles', subscription=subscription["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'GetCompletedFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetFailedFiles(self, times=1):         
        print "testGetFailedFiles"

        subscription = self.genSubscription(number=1)[0]

        filelist = self.genFiles(number=times)

        for i in range(times):     

            self.dao(classname='Subscriptions.FailFiles').execute(subscription=subscription["id"], file=filelist[i]["id"])

            time = self.perfTest(dao=self.dao, action='Subscriptions.GetFailedFiles', subscription=subscription["id"])
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'GetFailedFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testForFileset(self, times=1):         
        print "testForFileset"

        fileset = self.genFileset(number=1)[0]

        for i in range(times):     
            time = self.perfTest(dao=self.dao, action='Subscriptions.ForFileset', fileset=fileset.id)
            assert self.totaltime <= self.totalthreshold, 'ForFileset DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testNew(self, times=1):         
        #TestNew is taking longer than expected to run
        #TODO - Verify a possible overhead on this method
        print "testNew"

        workflow = self.genWorkflow(number=times, name='testNew')
        fileset = self.genFileset(number=times, name='testNew')

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Subscriptions.New', fileset=fileset[i].id, workflow=workflow[i].id, type='Merge')
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testLoad(self, times=1):         
        print "testLoad"

        subscription = self.genSubscription(number=1, name='testLoad')[0]

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Subscriptions.Load', workflow=subscription.getWorkflow().id, type='Processing', fileset=subscription.getFileset().id)
            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testJobs(self, times=1):         
        print "testJobs"

        subscription = self.genSubscription(number=1, name='testJobs')[0]

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Subscriptions.Jobs', subscription=subscription["id"])
            assert self.totaltime <= self.totalthreshold, 'Jobs DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testExists(self, times=1):         
        print "testExists"

        subscription = self.genSubscription(number=1, name='testExists')[0]

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Subscriptions.Exists', workflow=subscription.getWorkflow().id, fileset=subscription.getFileset().id, type='Merge')
            assert self.totaltime <= self.totalthreshold, 'Exists DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
