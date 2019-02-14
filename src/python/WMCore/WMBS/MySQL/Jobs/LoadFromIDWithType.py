#!/usr/bin/env python
"""
_LoadFromIDWithType_

MySQL implementation of LoadFromIDWithType

Created on Oct 3, 2012

@author: dballest
"""

from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID

class LoadFromIDWithType(LoadFromID):
    """
    _LoadFromIDWithType_

    Load jobs by ID but include the type
    of the subscription they belong to
    """

    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name,
                    wmbs_job_state.name AS state, wmbs_job.state_time, retry_count,
                    couch_record,  cache_dir, wmbs_location.site_name AS location,
                    outcome AS bool_outcome, fwjr_path AS fwjr_path,
                    wmbs_sub_types.name AS type
             FROM wmbs_job
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
             INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
             INNER JOIN wmbs_sub_types ON wmbs_sub_types.id = wmbs_subscription.subtype
             WHERE wmbs_job.id = :jobid"""
