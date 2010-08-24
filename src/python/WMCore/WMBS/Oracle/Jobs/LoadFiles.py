#!/usr/bin/env python
"""
_LoadFiles_

Oracle implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.2 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.LoadFiles import LoadFiles as LoadFilesJobMySQL

class LoadFiles(LoadFilesJobMySQL):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT fileid FROM wmbs_job_assoc WHERE JOB = :jobid"
