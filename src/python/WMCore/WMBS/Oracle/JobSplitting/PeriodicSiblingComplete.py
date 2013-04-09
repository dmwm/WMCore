"""
_PeriodicSiblingComplete_

Oracle implementation of JobSplitting.PeriodicSiblingComplete
"""

from WMCore.WMBS.MySQL.JobSplitting.PeriodicSiblingComplete import PeriodicSiblingComplete as MySQLPeriodicSiblingComplete

class PeriodicSiblingComplete(MySQLPeriodicSiblingComplete):
    """
    Right now this is the same as the MySQL version.

    """
