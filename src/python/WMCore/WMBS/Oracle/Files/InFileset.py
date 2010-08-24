"""
Oracle implementation of Files.InFileset
"""
from WMCore.WMBS.MySQL.Files.InFileset import InFileset as InFilesetMySQL

class InFileset(InFilesetMySQL):
    sql = """SELECT DISTINCT id FROM wmbs_file_details WHERE id IN
             (SELECT fileid FROM wmbs_fileset_files WHERE fileset =
             (SELECT id FROM wmbs_fileset WHERE name = :fileset))"""

