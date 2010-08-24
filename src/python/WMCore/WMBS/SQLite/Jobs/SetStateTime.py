#!/usr/bin/env python
"""
_SetStateTime_

SQLite implementation of Jobs.SetStateTime
"""




from WMCore.WMBS.MySQL.Jobs.SetStateTime import SetStateTime as MySQLSetStateTime

class SetStateTime(MySQLSetStateTime):
    pass
