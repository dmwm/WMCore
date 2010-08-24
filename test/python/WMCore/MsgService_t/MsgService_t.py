#!/usr/bin/env python
"""
_MsgService_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: MsgService_t.py,v 1.2 2008/08/26 13:55:16 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"

import commands
import unittest
import logging
import os
import threading

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

from WMCore.WMFactory import WMFactory

class MsgServiceTest(unittest.TestCase):
    """
    _MsgService_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """

    _setup = False
    _teardown = True

    def setUp(self):
        "make a logger instance and create tables"
       
        if not MsgServiceTest._setup: 
            logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('MsgServiceTest')
            myThread.dialect = 'MySQL'
        
            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("MYSQLDATABASE"), \
                options)
        
            myThread.dbi = dbFactory.connect() 

            factory = WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute()
            if createworked:
                logging.debug("MsgService tables created")
            else:
                logging.debug("MsgService tables could not be created, \
                    already exists?")
                                              
            MsgServiceTest._setup = True

    def tearDown(self):
        """
        Delete the databases
        """
        myThread = threading.currentThread()
        if MsgServiceTest._teardown:
            myThread.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root --socket='+os.getenv("DBSOCK")+' drop '+os.getenv("DBNAME")))
            myThread.logger.debug(commands.getstatusoutput('mysqladmin -u root --socket='+os.getenv("DBSOCK")+' create '+os.getenv("DBNAME")))
            myThread.logger.debug("database deleted")
               
               
    def testSubscribe(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        self.msgService = \
            myThread.factory['msgService'].loadObject("MsgService")
        self.msgService.registerAs("TestComponent")
        myThread.transaction.commit()
        
if __name__ == "__main__":
    unittest.main()
