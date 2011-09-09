#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Oracle implementation of ResourceControl.ListThresholdsForCreate
"""

from WMCore.ResourceControl.MySQL.ListThresholdsForCreate \
     import ListThresholdsForCreate as MySQLListThresholdsForCreate

class ListThresholdsForCreate(MySQLListThresholdsForCreate):

    assignedSQL = """SELECT wmbs_location.site_name, wmbs_location.job_slots, wmbs_location.cms_name,
                            COUNT(wmbs_job.id) AS total FROM wmbs_job
                       INNER JOIN wmbs_job_state ON
                         wmbs_job.state = wmbs_job_state.id
                       INNER JOIN wmbs_location ON
                         wmbs_job.location = wmbs_location.id
                     WHERE wmbs_job_state.name != 'success' AND
                           wmbs_job_state.name != 'complete' AND
                           wmbs_job_state.name != 'exhausted' AND
                           wmbs_job_state.name != 'cleanout' AND
                           wmbs_job_state.name != 'killed'
                     GROUP BY wmbs_location.site_name, wmbs_location.job_slots, wmbs_location.cms_name"""
