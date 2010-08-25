#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer: Oracle version

"""
__revision__ = "$Id: NewDataset.py,v 1.1 2009/05/15 16:19:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions
from WMComponent.DBSBuffer.Database.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):
    """
    _DBSBuffer.NewDataset_
    
    Add a new dataset to DBS Buffer: Oracle version
    
    """

    def execute(self, dataset=None, algoInDBS=0, conn=None, transaction = False):
        binds = self.getBinds(dataset, algoInDBS)

	try:
            result = self.dbi.processData(self.sql, binds,
                                          conn = conn, transaction = transaction)
	except Exception, ex:
            #Ignore duplicate rows
            if ex.__str__().find("unique") != -1 :
                return
            else:
                raise ex
        return 
