#!/usr/bin/env python
"""
_CompleteInput_

SQLite implementation of Jobs.CompleteInput
"""

__revision__ = "$Id: CompleteInput.py,v 1.2 2010/04/28 16:28:37 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    pass
