#!/usr/bin/env python

"""
__WorkflowTest__

Performance testcase for WMBS Workflow class

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

class WorkflowTest(WMBSBase):
    """
    __WorkflowTest__

     Performance testcase for WMBS Workflow class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        """
            Common setUp for Workflow object DAO tests
            
        """
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'WorkflowPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self, dbf=dbf)

    def tearDown(self):
        """
            Common tearDown for Workflow object DAO tests
            
        """
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testExists(self, times=1):         
        """
            Testcase for the Workflow.Exists DAO class
            
        """
        print "testExists"

        list = self.genWorkflow(number=1)

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
       
        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Workflow.Exists', 
                                spec=list[0].spec, owner=list[0].owner, 
                                name=list[0].name)
            self.totaltime = self.totaltime + time                        
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
            time = self.perfTest(dao=self.dao, action='Workflow.New', 
                                spec="Test"+str(i), owner="PerformanceTestcase",
                                name="testNew"+str(i))
            self.totaltime = self.totaltime + time                        
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
            time = self.perfTest(dao=self.dao, action='Workflow.Delete', 
                   id=list[i].id)
            self.totaltime = self.totaltime + time                        
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

        list = self.genWorkflow(number=1)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Workflow.LoadFromID', 
                   workflow=list[0].id)
            self.totaltime = self.totaltime + time                        
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

        list = self.genWorkflow(number=1)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Workflow.LoadFromName', 
                   workflow=list[0].name)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'LoadFromName DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'

    def testLoadSpecOwner(self, times=1):         
        """
            Testcase for the Workflow.LoadSpecOwner DAO class
            
        """
        print "testLoadSpecOwner"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genWorkflow(number=times)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Workflow.LoadSpecOwner', 
                   spec=list[i].spec, owner=list[i].owner)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'LoadSpecOwner DAO '+\
                    'class - Operation too slow ( '+str(i+1)+' times, total '+\
                    'elapsed time:'+str(self.totaltime)+ \
                    ', threshold:'+str(self.totalthreshold)+' )'
