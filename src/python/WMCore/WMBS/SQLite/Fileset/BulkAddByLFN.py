"""
_BulkAddByLFN_

SQLite implementation of Fileset.BulkAddByLFN
"""





from WMCore.WMBS.MySQL.Fileset.BulkAddByLFN import BulkAddByLFN as MySQLBulkAddByLFN

class BulkAddByLFN(MySQLBulkAddByLFN):
    """
    _BulkAddByLFN_

    Bulk add multiple files to mupltiple filesets.  The file/fileset mappings
    are passed in as a list of dicts where each dict has two keys:
      lfn
      fileset
    """
