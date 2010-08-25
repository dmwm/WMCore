#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: Trigger_t.py,v 1.7 2009/10/13 23:00:07 meloam Exp $"
__version__ = "$Revision: 1.7 $"

import unittest
import os
import threading

from WMCore.Trigger.Trigger import Trigger
from WMCore.WMFactory import WMFactory

from WMQuality.TestInit import TestInit

class TriggerTest(unittest.TestCase):
    """
    _Trigger_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """
    # values for testing various sizes
    _triggers = 2
    _jobspecs = 5
    _flags = 4

    def setUp(self):
        "make a logger instance "
       
        # initialization necessary for proper style.
        myThread = threading.currentThread()
        myThread.dialect = None
        myThread.transaction = None


        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ['WMCore.Trigger'], \
            useDefault = False)

    def tearDown(self):
        """
        Database deletion 
        """
        self.testInit.clearDatabase()



    def testB(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        factory = WMFactory("trigger", "WMCore.Trigger")

        # perpare trigger name tables if working in multi queue
        myThread = threading.currentThread()
        trigger = Trigger()

        print("\nCreate Triggers")
        flags = []
        actions  = []
        for i in xrange(0, TriggerTest._triggers):
            for j in xrange(0, TriggerTest._jobspecs):
                for k in xrange(0, TriggerTest._flags):
                    flags.append({'trigger_id' : "trigger"+str(i), \
                                'id' : "jobSpec"+str(j), \
                                'flag_id' : "flag"+str(k)})
                payload = {'jobspec' : 'jobSpec'+str(j), \
                    'var1':'val1', 'var2':'val2','var3':'val3'}
                actions.append({'id' : "jobSpec"+str(j), \
                              'trigger_id' : "trigger" + str(i), \
                              'action_name' : "WMCore.Trigger.ActionTemplate",\
                              'payload': payload})

        myThread.transaction.begin()
        trigger.addFlag(flags)
        trigger.addFlag({'trigger_id' : 'single_insert', \
                         'id' : 'single_insert_id', \
                         'flag_id': 'single_flag_insert1'})
        trigger.addFlag({'trigger_id' : 'single_insert', \
                         'id' : 'single_insert_id', \
                         'flag_id': 'single_flag_insert2'})
        trigger.setAction(actions)
        myThread.transaction.commit()

    def testC(self):
        """
        Set almost all flags
        """

        print('testC')
        trigger = Trigger()
        myThread = threading.currentThread()
        myThread.transaction.begin()
        flags = []
        print("\nSet not all Flags")
        for i in xrange(0, TriggerTest._triggers):
            for j in xrange(0, TriggerTest._jobspecs):
                for k in xrange(0, (TriggerTest._flags-1)):
                    flags.append({'trigger_id' : "trigger" + str(i), \
                                 'id' : "jobSpec"+str(j), \
                                 'flag_id' : "flag"+str(k)})
        trigger.setFlag(flags)
        trigger.setFlag({'trigger_id' : 'single_insert', \
                          'id' : 'single_insert_id', \
                          'flag_id' : 'single_flag_insert1'})

        myThread.transaction.commit()

    def testD(self):
        """
        Set all flags and remove flags from database
        """
        print('testD')
        TriggerTest._teardown = True
        trigger = Trigger()
        myThread = threading.currentThread()
        myThread.transaction.begin()

        flags = []
        print("\nSet all Flags")
        for i in xrange(0, TriggerTest._triggers):
            for j in xrange(0, TriggerTest._jobspecs):
                for k in xrange(TriggerTest._flags-1, TriggerTest._flags):
                    flags.append({'trigger_id' : "trigger"+str(i), \
                                 'id' : "jobSpec"+str(j), \
                                 'flag_id' : "flag"+str(k)})
        trigger.setFlag(flags)
       
        myThread.transaction.commit()



if __name__ == "__main__":
    unittest.main()
