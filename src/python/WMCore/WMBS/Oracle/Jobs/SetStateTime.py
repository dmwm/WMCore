#!/usr/bin/env python
"""
_SetStateTime_

Oracle implementation of Jobs.SetStateTime
"""




from WMCore.WMBS.MySQL.Jobs.SetStateTime import SetStateTime as MySQLSetStateTime

class SetStateTime(MySQLSetStateTime):
    pass
