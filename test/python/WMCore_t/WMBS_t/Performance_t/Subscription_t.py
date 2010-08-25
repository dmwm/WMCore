#!/usr/bin/env python

"""
__SubscriptionTest__

Performance testcase for Subscription DAO class

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import logging
import unittest
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Subscription import Subscription

class SubscriptionTest(unittest.TestCase, WMBSBase):
    """
    __SubscriptionTest__

     Performance testcase for Subscription DAO class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self):
        """
            Common setUp for Subscription object DAO tests
            
        """
        #Call common setUp method from WMBSBase
                
        WMBSBase.setUp(self)

    def tearDown(self):
        """
            Common tearDown for Subscription object DAO tests
            
        """
        #Call superclass tearDown method
        WMBSBase.tearDown(self)


    def testNew(self, times=1):         
        """
            Testcase for the Subscription.New DAO class
            
        """
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        workflow = self.genWorkflow(number=times, name='testNew')
        fileset = self.genFileset(number=times, name='testNew')

        for i in range(times):
            startTime = time.time()    
            testSubscription = Subscription(fileset = fileset[i],
                                            workflow = workflow[i])
            testSubscription.create()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'New'+\
                    ' DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return

    def testExists(self, times=1):         
        """
            Testcase for the Subscription.Exists  class
            
        """
        print "testExists"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes


        subscriptions = self.genSubscription(number=times)

        for i in range(times):
            startTime = time.time()    
            testSubscription = Subscription(id = subscriptions[i]["id"])
            testSubscription.exists()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime 
            assert self.totaltime <= self.totalthreshold, 'New'+\
                    ' DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return


    def testDelete(self, times=1):         
        """
            Testcase for the Subscription.Exists  class
            
        """
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes


        subscriptions = self.genSubscription(number=times)

        for i in range(times):
            startTime = time.time()    
            testSubscription = Subscription(id = subscriptions[i]["id"])
            testSubscription.delete()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime 
            assert self.totaltime <= self.totalthreshold, 'New'+\
                    ' DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return


    def testLoadByID(self, times=1):         
        """
            Testcase for the Subscription.Load by ID class
            
        """
        print "testLoadByID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times, name='testLoad')

        for i in range(times):
            startTime = time.time()    
            testSubscription = Subscription(id = subscriptions[i]["id"])
            testSubscription.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime 
            assert self.totaltime <= self.totalthreshold, 'Load'+\
                    'DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'


        return


    def testLoadByDetails(self, times=1):         
        """
            Testcase for the Subscription.Load by Sub. info class
            
        """
        print "testLoadByDetails"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times, name='testLoad')

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(workflow = subscriptions[i].getWorkflow(), type='Processing',
                                            fileset = subscriptions[i].getFileset())
            testSubscription.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime 
            assert self.totaltime <= self.totalthreshold, 'Load'+\
                    'DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'


        return


    def testFilesOfStatus(self, times=1):         
        """
            Testcase for the Subscription.FilesOfStatus class using failed failes
            
        """
        print "testFilesOfStatus"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times)
       
        filelist = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            failedFiles = testSubscription.filesOfStatus(status = "Failed")
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            #time = self.perfTest(dao=self.dao, 
            #    action='Subscriptions.FailFiles', 
            #    subscription=subscription["id"],
            #    file=filelist[i]["id"])
            #self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'FailFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return


    def testAcquireFiles(self, times=1):         
        """
            Testcase for the Subscription.AcquireFiles DAO class
            
        """
        print "testAcquireFiles"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times)
       
        filelist = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            acquiredFiles = testSubscription.filesOfStatus(status = "Acquired")
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'AcquireFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return

    def testGetAvailableFiles(self, times=1):         
        """
            Testcase for the Subscription.GetAvailableFiles DAO class
            
        """
        print "testGetAvailableFiles"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times)

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            availableFiles = testSubscription.availableFiles()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetAvailable'+\
                    'Files DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return

    def testCompleteFiles(self, times=1):         
        """
            Testcase for the Subscription.CompleteFiles DAO class
            
        """
        print "testCompleteFiles"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times)
       
        filelist = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            completedFiles = testSubscription.filesOfStatus(status = "Completed")
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'CompleteFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return


    def testFailedFiles(self, times=1):         
        """
            Testcase for the Subscription.FailedFiles DAO class
            
        """
        print "testFailedFiles"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times)
       
        filelist = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            failedFiles = testSubscription.filesOfStatus(status = "Failed")
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'CompleteFiles DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return


    def testJobs(self, times=1):         
        """
            Testcase for the Subscription.Jobs DAO class
            
        """
        print "testJobs"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        subscriptions = self.genSubscription(number=times, name='testJobs')

        for i in range(times):
            startTime = time.time()
            testSubscription = Subscription(id = subscriptions[i]["id"])
            testSubscription.getJobs()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'Jobs'+\
                    ' DAO class - Operation too slow ( '+str(i+1)+\
                    ' times, total elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

        return
    























# Functionality not present in Subscription.py
# -mnorman

#    def testDeleteAcquiredFiles(self, times=1):         
#        """
#            Testcase for the Subscription.DeleteAcquiredFiles DAO class
#            
#        """
#        print "testDeleteAcquiredFiles"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        subscription = self.genSubscription(number=1)[0]
#       
#        filelist = self.genFiles(number=times)
#
#        for i in range(times):     
#
#            self.dao(classname='Subscriptions.AcquireFiles').\
#                execute(subscription=subscription["id"], file=filelist[i]["id"])
#
#            time = self.perfTest(dao=self.dao, 
#                   action='Subscriptions.DeleteAcquiredFiles', 
#                   subscription=subscription["id"],file=filelist[i]["id"])
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'DeleteAcquired'+\
#                    'Files DAO class - Operation too slow ( '+str(i+1)+\
#                    ' times, total elapsed time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'


#I believe the functionality of these three is now in the filesOfStatus functionality
# -mnorman

#    def testGetAcquiredFiles(self, times=1):         
#        """
#            Testcase for the Subscription.GetAcquiredFiles DAO class
#            
#        """
#        print "testGetAcquiredFiles"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        subscription = self.genSubscription(number=1)[0]
#
#        filelist = self.genFiles(number=times)
#
#        for i in range(times):     
#
#            self.dao(classname='Subscriptions.AcquireFiles').\
#                execute(subscription=subscription["id"], file=filelist[i]["id"])
#
#            time = self.perfTest(dao=self.dao, 
#                   action='Subscriptions.GetAcquiredFiles', 
#                   subscription=subscription["id"])
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'GetAcquired'+\
#                    'Files DAO class - Operation too slow ( '+str(i+1)+\
#                    ' times, total elapsed time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'
#
#
#
#    def testGetCompletedFiles(self, times=1):         
#        """
#            Testcase for the Subscription.GetCompletedFiles DAO class
#            
#        """
#        print "testGetCompletedFiles"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        subscription = self.genSubscription(number=1)[0]
#
#        filelist = self.genFiles(number=times)
#
#        for i in range(times):     
#
#            self.dao(classname='Subscriptions.CompleteFiles').\
#                execute(subscription=subscription["id"], file=filelist[i]["id"])
#
#            time = self.perfTest(dao=self.dao, 
#                action='Subscriptions.GetCompletedFiles', 
#                subscription=subscription["id"])
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'GetCompleted'+\
#                    'Files DAO class - Operation too slow ( '+str(i+1)+\
#                    ' times, total elapsed time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'            
#
#    def testGetFailedFiles(self, times=1):         
#        """
#            Testcase for the Subscription.GetFailedFiles DAO class
#            
#        """
#        print "testGetFailedFiles"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        subscription = self.genSubscription(number=1)[0]
#
#        filelist = self.genFiles(number=times)
#
#        for i in range(times):     
#
#            self.dao(classname='Subscriptions.FailFiles').\
#                execute(subscription=subscription["id"], file=filelist[i]["id"])
#
#            time = self.perfTest(dao=self.dao, 
#                action='Subscriptions.GetFailedFiles', 
#                subscription=subscription["id"])
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'GetFailed'+\
#                    'Files DAO class - Operation too slow ( '+str(i+1)+\
#                    ' times, total elapsed time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'

# Deprecated, as command is not used in wrapper class

#    def testForFileset(self, times=1):         
#        """
#            Testcase for the Subscription.ForFileset DAO class
#            
#        """
#        print "testForFileset"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        fileset = self.genFileset(number=1)[0]
#
#        for i in range(times):     
#            time = self.perfTest(dao=self.dao, 
#                action='Subscriptions.ForFileset', 
#                fileset=fileset.id)
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'ForFileset'+\
#                    ' DAO class - Operation too slow ( '+str(i+1)+\
#                    ' times, total elapsed time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'






if __name__ == "__main__":
    unittest.main()
