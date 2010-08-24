#!/usr/bin/env python
"""
_MsgService_unit_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: MsgService_t.py,v 1.1 2008/08/20 08:21:44 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"

import commands
import unittest
import logging
import os

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory

from WMCore.MsgService.MsgService import MsgService


class MsgServiceTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance and create tables"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')
        
        self.logger = logging.getLogger('MsgServiceTest')
        
        
        self.tearDown()
        
        options = {}
        options['unix_socket'] = os.getenv("DBSOCK")
        self.dbf = DBFactory(self.logger, os.getenv("MYSQLDATABASE"), options)
        
        
        self.daofactory = DAOFactory(package='WMCore.MsgService', logger=self.logger, dbinterface=self.dbf.connect())
        
        SQLCreator = self.daofactory(classname='Create')
        createworked = SQLCreator.execute()
        if createworked:
            self.logger.debug("MsgService tables created")
        else:
            self.testlogger.debug("MsgService tables could not be created, already exists?")
            
                                              
    def tearDown(self):
        """
        Delete the databases
        """
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root --socket='+os.getenv("DBSOCK")+' drop '+os.getenv("DBNAME")))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root --socket='+os.getenv("DBSOCK")+' create '+os.getenv("DBNAME")))
        self.logger.debug("database deleted")
               
               
    def testSubscribe(self, notest=False):
        print("subscribing")
        self.msgService = MsgService(self.logger, self.dbf)
        
if __name__ == "__main__":
    unittest.main()
