#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Oracle implementation of ResourceControl.ListThresholdsForCreate
"""

__revision__ = "$Id: ListThresholdsForCreate.py,v 1.2 2010/07/01 19:52:08 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

from WMCore.ResourceControl.MySQL.ListThresholdsForCreate \
     import ListThresholdsForCreate as MySQLListThresholdsForCreate

class ListThresholdsForCreate(MySQLListThresholdsForCreate):
    unassignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots,
                              COUNT(job_location.location) AS total FROM wmbs_location
                         LEFT OUTER JOIN
                           (SELECT wmbs_job_assoc.job, wmbs_file_location.location AS location
                                   FROM wmbs_job_assoc
                              INNER JOIN wmbs_file_location ON
                                wmbs_job_assoc.fileid = wmbs_file_location.fileid
                              INNER JOIN wmbs_job ON
                                wmbs_job_assoc.job = wmbs_job.id
                            WHERE wmbs_job.location IS NULL    
                            GROUP BY wmbs_job_assoc.job, wmbs_file_location.location) job_location ON
                            wmbs_location.id = job_location.location
                       GROUP BY wmbs_location.site_name, wmbs_location.job_slots"""
