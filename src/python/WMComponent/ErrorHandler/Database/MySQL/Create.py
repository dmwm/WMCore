#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Create_

Class for creating MySQL specific schema for the error handler.

"""

__revision__ = "$Id: Create.py,v 1.1 2008/10/02 11:10:57 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating MySQL specific schema for persistent messages.
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}
        self.create['taa'] = """      
SET AUTOCOMMIT = 0; """
        self.create['ta_err_retries'] = """      
CREATE TABLE `err_retries` (
    `id` varchar(255) NOT NULL,
    `retries` int(11) NOT NULL default '0',
    PRIMARY KEY `id` (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""
 
