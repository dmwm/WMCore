'''n
Created on Aug 6, 2009

@author: meloam
'''
import unittest
import WMCore.Services.Requests as Requests
import os
import threading
import pprint
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from sets import Set
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Job import Job
from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.JobStateMachine.ChangeState import ChangeState, Transitions
from WMCore.JobStateMachine import DefaultConfig
from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.Job import Job
from WMCore.WMBS.Job import Job as WMBSJob
from WMQuality.TestInit import TestInit
import WMCore.Database.CMSCouch as CMSCouch

class testJSONRequests(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.request = Requests.JSONRequests()
    def roundTrip(self,data):
        encoded = self.request.encode(data)
        decoded = self.request.decode(encoded)
        self.assertEqual( data, decoded )
    
    def testSet1(self):
        self.roundTrip(Set([]))
    def testSet2(self):
        self.roundTrip(Set([1,2,3,4,Run(1)]))
    def testSet3(self):
        self.roundTrip(Set(['a','b','c','d']))
    def testSet4(self):
        self.roundTrip(Set([1,2,3,4,'a','b']))
    def testRun1(self):
        self.roundTrip(Run(1))
    def testRun2(self):
        self.roundTrip(Run(1,1))
    def testRun3(self):
        self.roundTrip(Run(1,2,3))
    def testMask1(self):
        self.roundTrip(Mask())
    def testMask2(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        self.roundTrip(mymask)
    def testMask3(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        myjob = Job()
        myjob["mask"] = mymask
        self.roundTrip(myjob)
    def testMask4(self):
        self.roundTrip({'LastRun': None, 'FirstRun': None, 'LastEvent': None, 'FirstEvent': None, 'LastLumi': None, 'FirstLumi': None})
    
    def testMask5(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        myjob = WMBSJob()
        myjob["mask"] = mymask
        self.roundTrip(myjob)
    
    def testMask6(self):
        mymask = Mask()
        myjob = WMBSJob()
        myjob["mask"] = mymask
        self.roundTrip(myjob)
                
#class TestWMBSJSON(unittest.TestCase):
#    transitions = None
#    change = None
#    def setUp(self):
#        """
#        _setUp_
#        """
#
#        self.transitions = Transitions()
#        self.testInit = TestInit(__file__)
#        self.testInit.setLogging()
#        self.testInit.setDatabaseConnection()
#    
#        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
#                                useDefault = False)
#
#        myThread = threading.currentThread()
#        daofactory = DAOFactory(package = "WMCore.WMBS",
#                                logger = myThread.logger,
#                                dbinterface = myThread.dbi)
#        
#        locationAction = daofactory(classname = "Locations.New")
#        locationAction.execute(siteName = "goodse.cern.ch")
#        locationAction.execute(siteName = "badse.cern.ch")
#                                
#        # if you want to keep from colliding with other people
#        #self.uniqueCouchDbName = 'jsm_test-%i' % time.time()
#        # otherwise
#        self.uniqueCouchDbName = 'jsm_test'
#        self.change = ChangeState(DefaultConfig.config, \
#                                  couchDbName=self.uniqueCouchDbName)
#        self.request = Requests.JSONRequests()

#
#    def tearDown(self):
#        """
#        _tearDown_
#        """
#
#        myThread = threading.currentThread()
#        factory = WMFactory("WMBS", "WMCore.WMBS")
#        destroy = factory.loadObject(myThread.dialect + ".Destroy")
#        myThread.transaction.begin()
#        destroyworked = destroy.execute(conn = myThread.transaction.conn)
#        server = CMSCouch.CouchServer(self.change.config.JobStateMachine.couchurl)
#        server.deleteDatabase(self.uniqueCouchDbName)
#        if not destroyworked:
#            raise Exception("Could not complete WMBS tear down.")
#        myThread.transaction.commit()
        
#    def testJob1(self):
#        self.roundTrip(Job())
#        
#    def testJobPackage1(self):
#        package = JobPackage()
#        package.append(1)
#        package.append(2)
#        package.append(3)
#        self.roundTrip(package)
#    
#    def roundTrip(self,data):
#        encoded = self.request.encode(data)
#        decoded = self.request.decode(encoded)
#        pp = pprint.PrettyPrinter()
#        datapp = pp.pformat(data)
#        decodedpp = pp.pformat(decoded)
#        encodedpp = pp.pformat(encoded)
#        self.assertEqual( data, decoded, "%s \n\n!= %s \n\n(encoded is %s)" %(datapp,decodedpp,encodedpp) )
#        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()