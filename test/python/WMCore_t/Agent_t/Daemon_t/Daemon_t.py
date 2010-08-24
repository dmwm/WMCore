#!/usr/bin/env python
"""
_Daemon_t_

Unit tests for  daemon creation

"""

__revision__ = "$Id: Daemon_t.py,v 1.3 2008/11/11 16:49:19 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "fvlingen@caltech.edu"

import commands
import unittest
import logging
import os
import threading
import time

from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMFactory import WMFactory

class DaemonTest(unittest.TestCase):
    """
    _Daemon_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """

    _setup = False
    _teardown = False

    # minimum number of messages that need to be in queue
    _minMsg = 20
    # number of publish and gets from queue
    _publishAndGet = 10

    def setUp(self):
        "make a logger instance and create tables"
       
        if not DaemonTest._setup: 
            logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('DaemonTest')
            myThread.dialect = 'MySQL'
        
            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)
        
            myThread.dbi = dbFactory.connect() 

            factory = WMFactory("daemon", "WMCore.Agent.Daemon."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute()
            if createworked:
                logging.debug("Daemon tables created")
            else:
                logging.debug("Daemon tables could not be created, \
                    already exists?")
                                              
            DaemonTest._setup = True

    def tearDown(self):
        """
        Deletion of the databases 
        """
        myThread = threading.currentThread()
        if DaemonTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        DaemonTest._teardown = False

               
    def testA(self):
        """
        __testSubscribe__

        Test deamon creation
        """
        # keep the parent alive
        pid = createDaemon(os.getenv("TESTDIR"), True)
        if pid != 0 :
            print('Deamon created I am the parent')
            time.sleep(2)
            print('Going to destroy my daemon')
            details = Details(os.path.join(os.getenv("TESTDIR"),"Daemon.xml"))
            print('Found Deamon details (sleeping for 10 secs.)')
            print(str(details.isAlive()))
            time.sleep(10)
            details.killWithPrejudice()
            print('Daemon killed')
        else:
            while True:
                logging.debug('I am a deamon (wait 10 seconds)')
                time.sleep(1)
        DaemonTest._teardown = True

    def runTest(self):
        """
        Runs the tests.
        """
        self.testA()

if __name__ == "__main__":
    unittest.main()


