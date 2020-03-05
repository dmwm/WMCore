#!/usr/bin/env python
"""
A page that knows how to format DB queries
"""




import os
import logging
import threading

from WMCore.WebTools.Page import TemplatedPage
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WebTools.ConfigDBMap import ConfigDBMap

class DatabasePage(TemplatedPage, DBFormatter):
    """
    A page that knows how to format DB queries
    """
    def __init__(self, config = {}):
        """
        __DatabasePage__

        A page with a database connection (a WMCore.Database.DBFormatter) held
        in self.dbi. Look at the DBFormatter class for other handy helper
        methods, such as getBinds and formatDict.

        The DBFormatter class was originally intended to be extensively
        sub-classed, such that it's subclasses followed the DAO pattern. For web
        tools we do not generally do this, and you will normally access the
        database interface directly:

        binds = {'id': 123}
        sql = "select * from table where id = :id"
        result = self.dbi.processData(sql, binds)
        return self.formatDict(result)

        Although following the DAO pattern is still possible and encouraged
        where appropriate. However, if you want to use the DAO pattern it may be
        better to *not* expose the DAO classes and have a normal DatabasePage
        exposed that passes the database connection to all the DAO's.
        """
        TemplatedPage.__init__(self, config)
        dbConfig = ConfigDBMap(config)
        conn = DBFactory(self, dbConfig.getDBUrl(), dbConfig.getOption()).connect()
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        DBFormatter.__init__(self, logger, conn)
        myThread = threading.currentThread()
        myThread.transaction = Transaction(conn)
        myThread.transaction.commit()
        return
