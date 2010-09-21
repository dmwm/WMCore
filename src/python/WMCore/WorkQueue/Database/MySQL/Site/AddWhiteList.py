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
        # multiple binds seem to work ok in other places, but not here, why? inner select seems to cause problems maybe?
        for i in binds: # Also this only seems to affect some setups? SQLAlchemy version? See #329
            self.dbi.processData(self.sql, i, conn = conn,
                                 transaction = transaction)
        return