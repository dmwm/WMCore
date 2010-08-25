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
# Framework for this code written automatically by Inspect.py


class TestChangeState(unittest.TestCase):
    _setup = False
    _teardown = False
    transitions = None
    change = None
    def setUp(self):
        """
        _setUp_
        """
        if self._setup:
            
            self.change = ChangeState(DefaultConfig.config)
            return
        self.transitions = Transitions()
        # TODO: write a config here
        
        # We need to set up the proper thread state for our test to run. Try it.
        self.testInit = TestInit( "TestChangeState" )
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema()
        self.change = ChangeState(DefaultConfig.config)
        self._setup = True


    def tearDown(self):
        """
        _tearDown_
        """
        self.testInit.clearDatabase()
        if self._teardown:
            return

        self._teardown = True


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




    def testRecordInCouch(self):
        """
        	This is the test class for function RecordInCouch from module ChangeState
        	"""
        CMSCouch.Database()
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
