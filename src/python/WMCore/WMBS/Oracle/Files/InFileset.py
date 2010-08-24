"""
Oracle implementation of Files.InFileset
"""
from WMCore.WMBS.MySQL.Files.InFileset import InFileset as InFilesetMySQL

class InFileset(InFilesetMySQL):
    sql = "SELECT DISTINCT fileid FROM wmbs_fileset_files WHERE fileset = :fileset"    


