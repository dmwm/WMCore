#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.1 2009/05/14 16:18:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
from WMComponent.DBSBuffer.Database.MySQL.NewDataset import NewDataset as MySQLNewDataset


class NewDataset(MySQLNewDataset):
    """
    _DBSBuffer.NewDataset_

    Add a new dataset to DBS Buffer

    """

    def GetNewDatasetDialect(self):

        return 'SQLite'


    def execute(self, dataset=None, algoInDBS=0, conn=None, transaction = False):
        binds = self.getBinds(dataset, algoInDBS)

	try:
        	result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

	except Exception, ex:
		if ex.__str__().find("unique") != -1 :
			#Ditch the duplicates
			return
		else:
			raise ex
        return 
