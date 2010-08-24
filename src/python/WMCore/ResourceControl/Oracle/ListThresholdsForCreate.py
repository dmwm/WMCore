#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Oracle implementation of ResourceControl.ListThresholdsForCreate
"""




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
                            WHERE wmbs_job.location IS NULL) job_location ON
                            wmbs_location.id = job_location.location
                       GROUP BY wmbs_location.site_name, wmbs_location.job_slots"""
