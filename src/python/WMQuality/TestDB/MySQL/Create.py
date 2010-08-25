#!/usr/bin/env python
"""
_Create_

Install the TestDB schema for MySQL.
"""

__revision__ = "$Id: Create.py,v 1.1 2010/03/01 16:49:02 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    def __init__(self, logger = None, dbi = None, params = None):
        """
        __init__

        """
        myThread = threading.currentThread()
        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)
        
        self.create["01test_tablea"] = \
          """CREATE TABLE test_tablea (
               column1 INTEGER,
               column2 INTEGER,
               column3 VARCHAR(255))"""

        self.create["01test_tableb"] = \
          """CREATE TABLE test_tableb (
               column1 VARCHAR(255),
               column2 INTEGER,
               column3 VARCHAR(255))"""

        self.create["01test_tablec"] = \
          """CREATE TABLE test_tablec (
               column1 VARCHAR(255),
               column2 VARCHAR(255),
               column3 VARCHAR(255))"""

        return
