#!/usr/bin/env python

"""
__WorkflowTest__

Performance testcase for WMBS Workflow class

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import unittest
import logging
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore_t.WMBS_t.Workflow_t import WorkflowTest as WorkflowTestBase
from nose.plugins.attrib import attr
class WorkflowTest(unittest.TestCase, WMBSBase):
    __performance__=True
    """
    __WorkflowTest__

     Performance testcase for WMBS Workflow class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self):
        """
            Common setUp for Workflow object DAO tests
            
        """
        #Call common setUp method from WMBSBase
                
        WMBSBase.setUp(self)

    def tearDown(self):
        """
            Common tearDown for Workflow object DAO tests
            
        """
        #Call superclass tearDown method
        WMBSBase.tearDown(self)
        return
    

#    def testCreateDeleteExistsPerformance(self, times=1):
#        """
#        Testcase for the Workflow Test class CreateDeleteExists function
#        
#        """
#
#        print "testCreateDeleteExists"
#
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        Tester = WorkflowTestBase()
#
#        for i in range(times):
#            startTime = time.time()
#            Tester.testCreateDeleteExists()
#            endTime = time.time()
#            elapsedTime = endTime - startTime   
#            self.totaltime = self.totaltime + elapsedTime  
#            assert self.totaltime <= self.totalthreshold, 'Exists DAO class -'+\
#                    ' Operation too slow ( '+str(i+1)+' times, total elapsed '+\
#                    'time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'
#        return

    def testExists(self, times=1):         
        """
            Testcase for the Workflow.Exists DAO class
            
        """
        print "testExists"



        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
            
        list = self.genWorkflow(number=times)      
 
        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(spec = list[i].spec, owner = list[i].owner, name = list[i].name, task="Test")
            testWorkflow.exists()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'Exists DAO class -'+\
                    ' Operation too slow ( '+str(i+1)+' times, total elapsed '+\
                    'time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testNew(self, times=1):         
        """
            Testcase for the Workflow.New DAO class
            
        """
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
       
        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(spec = "Test"+str(i), owner = "PerformanceTestcase", name = "testNew"+str(i), task="Test")
            testWorkflow.create()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'New DAO class - '+\
                    'Operation too slow ( '+str(i+1)+' times, total elapsed '+\
                    'time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testDelete(self, times=1):         
        """
            Testcase for the Workflow.Delete DAO class
            
        """
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)
       
        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(spec = list[i].spec, owner = list[i].owner, name = list[i].name)
            testWorkflow.delete()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'Delete DAO class -'+\
                    ' Operation too slow ( '+str(i+1)+' times, total elapsed '+\
                    'time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testLoadFromID(self, times=1):         
        """
            Testcase for the Workflow.LoadFromID DAO class
            
        """
        print "testLoadFromID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)

        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(id = list[i].id)
            testWorkflow.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'LoadFromID DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testLoadFromName(self, times=1):         
        """
            Testcase for the Workflow.LoadFromName DAO class
            
        """
        print "testLoadFromName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)

        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(name = list[i].name)
            testWorkflow.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'LoadFromName DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testLoadFromSpecOwner(self, times=1):         
        """
            Testcase for the Workflow.LoadSpecOwner DAO class
            
        """
        print "testLoadFromSpecOwner"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)

        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(spec = list[i].spec, owner = list[i].owner)
            testWorkflow.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'LoadFromSpecOwner DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testAddOutput(self, times=1):         
        """
            Testcase for the Workflow.AddOutput DAO class
            
        """
        print "testAddOutput"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)
        testFilesetA = Fileset(name = "testFilesetA")
        testFilesetA.create()

        for i in range(times):
            startTime = time.time()    
            testWorkflow = Workflow(spec = list[i].spec, owner = list[i].owner, name = list[i].name, task="Test")
            testWorkflow.create()
            testWorkflow.addOutput("testFilesetA", testFilesetA)
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'LoadFromSpecOwner DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'
        

if __name__ == "__main__":
    unittest.main()
