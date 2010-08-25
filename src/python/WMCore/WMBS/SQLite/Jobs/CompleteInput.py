#!/usr/bin/env python
"""
_CompleteInput_

SQLite implementation of Jobs.CompleteInput
"""

__revision__ = "$Id: CompleteInput.py,v 1.1 2009/10/13 20:52:41 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    """
    Identical to MySQL version for now.
    """
    pass
