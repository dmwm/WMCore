#!/usr/bin/env python
"""
_SetStateTime_

Oracle implementation of Jobs.SetStateTime
"""

__revision__ = "$Id: SetStateTime.py,v 1.1 2010/02/05 16:51:53 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.SetStateTime import SetStateTime as MySQLSetStateTime

class SetStateTime(MySQLSetStateTime):
    pass
