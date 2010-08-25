"""
SQLite implementation of site.UpdateDataSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateDataSiteMapping.py,v 1.1 2009/09/03 15:44:20 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Site.UpdateDataSiteMapping \
    import UpdateDataSiteMapping as UpdateDataSiteMappingMySQL

class UpdateDataSiteMapping(UpdateDataSiteMappingMySQL):
    deleteSql = UpdateDataSiteMappingMySQL.deleteSql
    insertSQL = UpdateDataSiteMappingMySQL.insertSQL
