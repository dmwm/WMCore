"""
_AddBlackList_

MySQL implementation of Site.New
"""

__all__ = []
__revision__ = "$Id: AddBlackList.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddBlackList(DBFormatter):
    sql = """INSERT IGNORE INTO wq_element_site_validation (element_id, site_id, valid) 
             VALUES (:element_id, (SELECT id FROM wq_site WHERE name = :site_name), 0)"""

    def execute(self, elementID, siteNames, conn = None, transaction = False):
        if type(siteNames) != list:
            siteNames = [siteNames]
        binds = [{"element_id": elementID, "site_name": name} for name in siteNames]
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
