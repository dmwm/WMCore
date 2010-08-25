"""
_BulkAddByLFN_

Oracle implementation of Fileset.BulkAddByLFN
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

    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT (SELECT id FROM wmbs_file_details WHERE lfn = :lfn), :fileset, :timestamp FROM DUAL"""
