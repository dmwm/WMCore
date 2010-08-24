#!/usr/bin/env python
"""
_DBSBuffer.NewDataset_

Add a new dataset to DBS Buffer

"""
__revision__ = "$Id: NewDataset.py,v 1.2 2008/10/16 09:46:49 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"


print "Dataset Path : " + \
                    "/"+aFile.dataset[0]['PrimaryDataset']+ \
                    "/"+aFile.dataset[0]['ProcessedDataset']+ \
                    "/"+aFile.dataset[0]['DataTier'] 


from WMCore.Database.DBFormatter import DBFormatter

class NewFile(DBFormatter):

    sql = """INSERT INTO dbsbuffer_dataset (Path)
                values (:path)"""

    def getBinds(self, dataset=None):
        # binds a list of dictionaries
        binds =  { 
                        'path': "/"+dataset['PrimaryDataset']+ \
                                "/"+dataset['ProcessedDataset']+ \
                                "/"+dataset['DataTier']
                }
        return binds

    def format(self, result):
        return True

    def execute(self, dataset=None, transaction = False):
        binds = self.getBinds(dataset)

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
