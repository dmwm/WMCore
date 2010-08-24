"""
SQLite implementation of site.UpdateBlockSiteMapping
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.UpdateBlockSiteMapping \
    import UpdateBlockSiteMapping as UpdateBlockSiteMappingMySQL

class UpdateBlockSiteMapping(UpdateBlockSiteMappingMySQL):
    deleteSql = UpdateBlockSiteMappingMySQL.deleteSql
    insertSQL = UpdateBlockSiteMappingMySQL.insertSQL
