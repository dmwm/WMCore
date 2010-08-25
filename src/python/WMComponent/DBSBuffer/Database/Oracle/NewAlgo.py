#!/usr/bin/env python
"""
_DBSBuffer.NewAlgo_

Add a new algorithm to DBS Buffer: Oracle version

"""
__revision__ = "$Id: NewAlgo.py,v 1.1 2009/05/15 16:19:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions
from WMComponent.DBSBuffer.Database.MySQL.NewAlgo import NewAlgo as MySQLNewAlgo

class NewAlgo(MySQLNewAlgo):
    """
    _DBSBuffer.NewAlgo_

    Add a new algorithm to DBS Buffer: Oracle version

    """

    def GetNewAlgoDialect(self):

        return 'Oracle'

    def execute(self, dataset=None, conn=None, transaction = False):
        binds = self.getBinds(dataset)

        try:
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        except Exception, ex:
            #Ignore duplicate results
            if ex.__str__().find("unique") != -1 :
                return
            else:
                raise ex
        return 
