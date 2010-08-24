#!/usr/bin/env python
"""
_LoadFiles_

Oracle implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.1 2008/11/24 21:51:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFiles import LoadFiles as LoadFilesJobMySQL

class LoadFiles(LoadFilesJobMySQL):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT fileid FROM wmbs_job_assoc WHERE JOB = :jobid"