#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.1 2008/10/15 14:29:08 afaq Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"


from WMCore.Database.DBFormatter import DBFormatter

class NewFile(DBFormatter):

    sql = """INSERT INTO dbsbuffer_dataset (Path)
                values (:path)"""

    def getBinds(self, dataset=None):
        # binds a list of dictionaries
        binds =  { 
                        'path': dataset['path']
                }

        return binds

    def format(self, result):
        return True

    def execute(self, dataset=None, transaction = False):
        binds = self.getBinds(dataset)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
