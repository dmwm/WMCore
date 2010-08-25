"""
SQLite implementation of site.UpdateBlockSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateBlockSiteMapping.py,v 1.1 2009/08/18 23:18:16 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.UpdateBlockSiteMapping \
    import UpdateBlockSiteMapping as UpdateBlockSiteMappingMySQL

class UpdateBlockSiteMapping(UpdateBlockSiteMappingMySQL):
    deleteSql = UpdateBlockSiteMappingMySQL.deleteSql
    insertSQL = UpdateBlockSiteMappingMySQL.insertSQL
