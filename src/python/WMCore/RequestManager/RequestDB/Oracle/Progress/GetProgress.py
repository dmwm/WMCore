from WMCore.RequestManager.RequestDB.MySQL.Progress.GetProgress import GetProgress as GetProgressMySQL

class GetProgress(GetProgressMySQL):
    sql = """SELECT * FROM reqmgr_progress_update 
                  WHERE ROWNUM <= 1 AND request_id=:request_id"""

