"""
_IsCompleteOnRun_
SQLite implementation of Subscriptions.IsCompleteOnRun

Checks all files in the given subscription and given run are completed.
"""
__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.IsCompleteOnRun import IsCompleteOnRun as IsCompleteOnRunMySQL

class IsCompleteOnRun(IsCompleteOnRunMySQL):
    """
    _IsCompleteOnRun_
    
    Return number of files for available, complete, failed status
    for a given run and a given subscription.
    
    TODO: can use left outer join to check the completeness.
    Not sure join is more expensive than multiple select with count
    """
    sql = IsCompleteOnRunMySQL.sql