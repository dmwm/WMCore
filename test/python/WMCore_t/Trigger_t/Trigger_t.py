#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: Trigger_t.py,v 1.3 2008/10/29 13:21:50 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"

import commands
import unittest
import logging
import os
import threading

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.Trigger.Trigger import Trigger
from WMCore.WMFactory import WMFactory


class TriggerTest(unittest.TestCase):
    """
    _Trigger_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """

    _setup = False
    _teardown = False
    # values for testing various sizes
    _triggers = 2
    _jobspecs = 5
    _flags = 4

    def setUp(self):
        "make a logger instance "
       
        if not TriggerTest._setup: 
            print('trigger setup (once)')
            logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            TriggerTest._setup = True

    def tearDown(self):
        """
        Database deletion 
        """
        myThread = threading.currentThread()
        if TriggerTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        if TriggerTest._teardown and myThread.dialect == 'Oracle':
            factory = WMFactory("trigger", "WMCore.Trigger")
            destroy = factory.loadObject(myThread.dialect+".Destroy")
            myThread.transaction.begin()
            destroyworked = destroy.execute(conn = myThread.transaction.conn)
            if not destroyworked:
                raise Exception("MsgService tables could not be destroyed")
            myThread.transaction.commit()


        TriggerTest._teardown = False

    def testA(self):
        "create tables"
        print('testA')
        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('TriggerTest')
        myThread.dialect = os.getenv('DIALECT')
        
        options = {}
        if myThread.dialect == 'MySQL':
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)
        else:
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"))
    
        myThread.dbi = dbFactory.connect() 

        factory = WMFactory("trigger", "WMCore.Trigger")
        create = factory.loadObject(myThread.dialect+".Create")
        myThread.transaction = Transaction(myThread.dbi)
        createworked = create.execute(conn = myThread.transaction.conn)
        if not createworked:
            raise Exception("Trigger tables could not be created, \
                already exists?")
        myThread.transaction.commit()                                  

    def testB(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        print('testB')
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


    def runTest(self):
        self.testA() 
        self.testB() 
        self.testC() 
        self.testD() 

if __name__ == "__main__":
    unittest.main()
