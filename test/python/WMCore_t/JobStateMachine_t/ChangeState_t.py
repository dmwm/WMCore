#!/usr/bin/python

import unittest
import sys
import os
import logging
import threading

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobStateMachine.ChangeState import ChangeState, Transitions
from WMCore.JobStateMachine import DefaultConfig
import WMCore.Database.CMSCouch as CMSCouch
import time
# Framework for this code written automatically by Inspect.py


class TestChangeState(unittest.TestCase):

    transitions = None
    change = None
    def setUp(self):
        """
        _setUp_
        """
        self.transitions = Transitions()
        # TODO: write a config here
        
        # We need to set up the proper thread state for our test to run. Try it.

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        try:
            self.testInit.setSchema(customModules = ["WMCore.ThreadPool"], useDefault = False)
        except:
            factory = WMFactory("Threadpool", "WMCore.ThreadPool")
            destroy = factory.loadObject(myThread.dialect + ".Destroy")
            destroyworked = destroy.execute(conn = myThread.transaction.conn)

                                
        # if you want to keep from colliding with other people
        #self.uniqueCouchDbName = 'jsm_test-%i' % time.time()
        # otherwise
        self.uniqueCouchDbName = 'jsm_test'
        self.change = ChangeState(DefaultConfig.config, \
                                  couchDbName=self.uniqueCouchDbName)


    def tearDown(self):
        """
        _tearDown_
        """

        myThread = threading.currentThread()
        


        factory = WMFactory("Threadpool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)

        server = CMSCouch.CouchServer(self.change.config.JobStateMachine.couchurl)
        server.deleteDatabase(self.uniqueCouchDbName)
        


    def testCheck(self):
    	"""
    	This is the test class for function Check from module ChangeState
    	"""
        # Run through all good state transitions and assert that they work
        for state in self.transitions.keys():
            for dest in self.transitions[state]:
                self.change.check(dest, state)
        dummystates = ['dummy1', 'dummy2', 'dummy3', 'dummy4']

        # Then run through some bad state transistions and assertRaises(AssertionError)
        for state in self.transitions.keys():
            for dest in dummystates:
                self.assertRaises(AssertionError, self.change.check, dest, state)
    	return

    def testPersist(self):
    	"""
    	This is the test class for function Persist from module ChangeState
    	"""
        return


    def testPropagate(self):
    	"""
    	This is the test class for function Propagate from module ChangeState
    	"""
        return




    def testRecordOneInCouch(self):
        """
        	This is the test class for function RecordInCouch from module ChangeState
        	"""
        jsm = self.change.recordInCouch( [{ "dumb_value": "is_dumb" }], "new", "none")
        print jsm
        jsm = self.change.recordInCouch( jsm , "created", "new")
        print jsm
        jsm = self.change.recordInCouch( jsm , "executing", "created")
        print jsm
        jsm = self.change.recordInCouch( jsm , "complete", "executing")
        print jsm
        jsm = self.change.recordInCouch( jsm , "success", "complete")
        print jsm
        jsm = self.change.recordInCouch( jsm , "closeout", "success")
        print jsm
        jsm = self.change.recordInCouch( jsm , "cleanout", "closeout")
        
        # now, walk up the chain of couch_records to follow the path the
        #  job took through the state machine
        
        print jsm
        
            
        return




    def testStates(self):
    	"""
    	This is the test class for function States from module ChangeState
    	"""
        return




if __name__ == "__main__":
#export DATABASE="mysql://sfoulkes:@localhost/ProdAgentDB_sfoulkes"
#export DIALECT="MySQL"
#export DBSOCK="/var/lib/mysql/mysql.sock"
    unittest.main()
