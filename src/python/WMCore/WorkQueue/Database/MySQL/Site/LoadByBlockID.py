"""
_New_

MySQL implementation of Site.LoadByBlockID
"""

__all__ = []
__revision__ = "$Id: LoadByBlockID.py,v 1.1 2009/06/15 20:57:03 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByBlockID(DBFormatter):
    sql = """SELECT st.name FROM site st
                INNER JOIN wq_block_site_assoc bsa ON (bsa.site_id = st.id) 
                WHERE bsa.block_id = :blockID    
          """

    def execute(self, blockID, conn = None, transaction = False):
        binds = {"blockID": blockID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
