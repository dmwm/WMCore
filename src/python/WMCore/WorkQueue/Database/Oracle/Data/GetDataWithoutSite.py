"""

Orcale implementation of Block.DataWithoutSite
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Data.GetDataWithoutSite \
    import GetDataWithoutSite as GetDataWithoutSiteMySQL

class GetDataWithoutSite(GetDataWithoutSiteMySQL):
    pass
