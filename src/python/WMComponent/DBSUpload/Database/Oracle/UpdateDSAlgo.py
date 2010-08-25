#!/usr/bin/env python
"""
_DBSBuffer.UpdateDSFileCount_

Update Algo status in a Dataset to promoted

"""
__revision__ = "$Id: UpdateDSAlgo.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter
from WMComponent.DBSUpload.Database.MySQL.UpdateDSAlgo import UpdateDSAlgo as MySQLUpdateDSAlgo

class UpdateDSAlgo(MySQLUpdateDSAlgo):

    """
    Oracle implementation to update algo status in dataset
    """
            
