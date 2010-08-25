"""
_AddWhiteList_

MySQL implementation of Site.AddWhiteList
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class AddWhiteList(DBFormatter):
    sql = """INSERT IGNORE INTO wq_element_site_validation (element_id, site_id, valid) 
             VALUES (:element_id, (SELECT id FROM wq_site WHERE name = :site_name), 1)"""
             
    def execute(self, elementID, siteNames, conn = None, transaction = False):
        if type(siteNames) != list:
            siteNames = [siteNames]
        binds = [{"element_id": elementID, "site_name": name} for name in siteNames]
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
