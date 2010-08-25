"""
_New_

MySQL implementation of Site.LoadByBlockID
"""

__all__ = []
__revision__ = "$Id: LoadByDataID.py,v 1.1 2010/04/07 19:17:08 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByDataID(DBFormatter):
    sql = """SELECT st.name FROM wq_site st
                INNER JOIN wq_data_site_assoc dsa ON (dsa.data_id = st.id)
                WHERE dsa.data_id = :dataID
          """

    def execute(self, blockID, conn = None, transaction = False):
        binds = {"dataID": blockID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatOne(results)
