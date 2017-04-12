"""
Oracle implementation of AddRunLumiFile
"""
from WMCore.WMBS.MySQL.Files.AddRunLumi import AddRunLumi as AddRunLumiMySQL

class AddRunLumi(AddRunLumiMySQL):
    """
    _AddRunLumi_

    overwirtes MySQL Files.AddRunLumi.sql to use in oracle.

    To Check: might not need to overwrite if modifiy MySQL.Files.AddRunLumi.sql
    a bit to work with both
    """
    sql = """INSERT INTO wmbs_file_runlumi_map (fileid, run, lumi, num_events)
                SELECT wfd.id, :run, :lumi, :num_events FROM wmbs_file_details wfd WHERE lfn = :lfn
                  AND NOT EXISTS (SELECT fileid FROM wmbs_file_runlumi_map wfrm2
                                   WHERE wfrm2.fileid = wfd.id
                                   AND wfrm2.run = :run
                                   AND wfrm2.lumi = :lumi)"""
