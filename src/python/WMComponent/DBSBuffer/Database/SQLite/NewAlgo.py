#!/usr/bin/env python
"""
_DBSBuffer.NewAlgo_

Add a new algorithm to DBS Buffer

"""
__revision__ = "$Id: NewAlgo.py,v 1.1 2009/05/14 16:18:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMComponent.DBSBuffer.Database.MySQL.NewAlgo import NewAlgo as MySQLNewAlgo

class NewAlgo(MySQLNewAlgo):
    """
    _DBSBuffer.NewAlgo_

    Add a new algorithm to DBS Buffer

    """

    def GetNewAlgoDialect(self):

        return 'SQLite'


    def execute(self, dataset=None, conn=None, transaction = False):
        binds = self.getBinds(dataset)

        try:
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        except Exception, ex:
            if ex.__str__().find("unique") != -1 :
                #Ditch duplicate entries
                return
            else:
                raise ex
        return 
