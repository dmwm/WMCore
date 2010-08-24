"""
SQLite implementation of site.UpdateBlockSiteMapping
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.UpdateDataSiteMapping \
    import UpdateDataSiteMapping as UpdateDataSiteMappingMySQL

class UpdateDataSiteMapping(UpdateDataSiteMappingMySQL):
    deleteSql = UpdateDataSiteMappingMySQL.deleteSql.replace('IGNORE', 'OR IGNORE', 1)
    insertSQL = UpdateDataSiteMappingMySQL.insertSQL.replace('IGNORE', 'OR IGNORE', 1)
