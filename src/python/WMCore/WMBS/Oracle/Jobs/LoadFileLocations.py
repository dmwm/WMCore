#!/usr/bin/env python
"""
_LoadFileLocations_

Oracle implementation of Jobs.LoadFileLocations
"""

__all__ = []
__revision__ = "$Id: LoadFileLocations.py,v 1.1 2009/10/15 19:41:06 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFileLocations import LoadFileLocations as MySQLLoadFileLocations

class LoadFileLocations(MySQLLoadFileLocations):
    """
    _LoadFileLocations_

    Retrieve all locations for a given job
    NOTE: THIS ASSUMES THAT ALL FILES HAVE IDENTICAL LOCATIONS!
    """

    sql = """SELECT DISTINCT wl.site_name FROM wmbs_location wl
               INNER JOIN wmbs_file_location wfl ON wfl.location = wl.id
               INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfl.fileid
               WHERE wja.job = :jobid
    """
