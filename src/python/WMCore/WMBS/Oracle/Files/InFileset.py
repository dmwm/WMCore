"""
Oracle implementation of Files.InFileset
"""
from WMCore.WMBS.MySQL.Files.InFileset import InFileset as InFilesetMySQL

class InFileset(InFilesetMySQL):
    
    sql = """ select distinct fileD.id, fileD.lfn, fileD.filesize, fileD.events, 
                              map.run, map.lumi
              from wmbs_file_details fileD 
              inner join wmbs_file_runlumi_map map on (map.fileid = fileD.id) 
              where id in (select fileid from wmbs_fileset_files where 
              fileset = (select id from wmbs_fileset where name = :fileset))"""
                