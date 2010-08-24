#!/usr/bin/env python

"""
__FilesetTest__

Performance testcase for WMBS Fileset class

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

class FilesetTest(WMBSBase):
    """
    __FilesetTest__

     Performance testcase for WMBS Fileset class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    def setUp(self, sqlURI='', logarg=''):
        """
            Common setUp for Fileset object DAO tests
            
        """                
        self.logger = logging.getLogger(logarg + 'FilesetPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)

        #Call superclass setUp method        
        WMBSBase.setUp(self, dbf = dbf)

    def tearDown(self):
        """
            Common tearDown for Fileset object DAO tests
            
        """
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testNew(self, times=1):
        """
            Testcase for the Fileset.New DAO class
            
        """
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
        
        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Fileset.New', 
                                name="TestFileset"+str(i))
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'New DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testDelete(self, times=1):
        """
            Testcase for the Fileset.Delete DAO class
            
        """
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Fileset.Delete', 
                                name=list[i].name)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'Delete DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testExists(self, times=1):
        """
            Testcase for the Fileset.Exists DAO class
            
        """
        print "testExists"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Fileset.Exists', 
                                name=list[i].name)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'Exists DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testLoadFromID(self, times=1):
        """
            Testcase for the Fileset.LoadFromID DAO class
            
        """
        print "testLoadFromID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Fileset.LoadFromID', 
                                fileset=list[i].id)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'LoadFromID DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testLoadFromName(self, times=1):
        """
            Testcase for the Fileset.LoadFromName DAO class
            
        """
        print "testLoadFromName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Fileset.LoadFromName', 
                                fileset=list[i].name)
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'LoadFromName DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    #Waiting for fileset parentage to be needed 

#    def testParentage(self):
#        print "testParentage"

#        childname = "ChildFileset1234"

#        childFileset = Fileset(name=childname,                         
#                        logger=self.logger, 
#                        dbfactory=self.dbf) 
#        childFileset.create()

        #Add the child fileset to the DB
        #self.dao(classname='Fileset.New').execute(name=childname)
        
