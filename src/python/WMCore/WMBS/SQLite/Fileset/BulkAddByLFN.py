"""
_BulkAddByLFN_

SQLite implementation of Fileset.BulkAddByLFN
"""

__revision__ = "$Id: BulkAddByLFN.py,v 1.1 2010/03/09 18:29:58 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.WMBS.MySQL.Fileset.BulkAddByLFN import BulkAddByLFN as MySQLBulkAddByLFN

class BulkAddByLFN(MySQLBulkAddByLFN):
    """
    _BulkAddByLFN_

    Bulk add multiple files to mupltiple filesets.  The file/fileset mappings
    are passed in as a list of dicts where each dict has two keys:
      lfn
      fileset
    """
