#!/usr/bin/env python
"""
_DBSBuffer.UpdateFileStatus_

Update file status to promoted

"""
__revision__ = "$Id: UpdateFilesStatus.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.UpdateFilesStatus import UpdateFilesStatus as MySQLUpdateFilesStatus

class UpdateFilesStatus(MySQLUpdateFilesStatus):

    """
Oracle implementation to update file status to promoted
    """
            
