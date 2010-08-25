#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.2 2009/07/14 19:15:46 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):
    """
    _NewDataset_

    Add a new dataset to DBS Buffer
    """
    pass
