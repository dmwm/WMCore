#!/usr/bin/env python
"""
_GetJobGroups_

MySQL implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.5 2009/09/28 20:19:03 mnorman Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.JobGroup import JobGroup
class GetJobGroups(DBFormatter):

    sql = """SELECT wmbs_jobgroup.id FROM wmbs_jobgroup
              WHERE wmbs_jobgroup.subscription = :subscription
    """
    
    def execute(self, subscription = None, conn = None, transaction = False):

        result = self.dbi.processData(self.sql, {"subscription": subscription}, \
                                      conn = conn, transaction = transaction)

        #For some reason, this returns a list of lists; I think
        #Doesn't seem to bother people yet
        return self.format(result)



    
  
