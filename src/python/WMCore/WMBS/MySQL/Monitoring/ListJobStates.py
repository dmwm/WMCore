#!/usr/bin/env python
"""
_ListJobStates_

Retrieve a list of all possible job states from WMBS.
"""

__revision__ = "$Id: ListJobStates.py,v 1.1 2010/01/25 20:42:37 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListJobStates(DBFormatter):
    sql = "SELECT name FROM wmbs_job_state"
    
    def format(self, result):
        """
        _format_

        Format the results into a single list.
        """
        results = DBFormatter.format(self, result)

        resultList = []
        for result in results:
            resultList.append(result[0])
            
        return resultList
        
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
