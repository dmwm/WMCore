"""
_UpdateLocation_

MySQL implementation of Jobs.UpdateLocation

Created on Apr 17, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateLocation(DBFormatter):
    """
    _UpdateLocation_

    Update the location of the job in WMBS, must be a valid
    site in wmbs_location.
    """

    sql = """
          UPDATE wmbs_job
          SET location = COALESCE((SELECT id FROM wmbs_location WHERE cms_name = :location), location)
          WHERE id = :jobID
          """

    def execute(self, jobs, conn = None, transaction = False):
        binds = []
        for job in jobs:
            bind = {'jobID' : job['jobid'],
                    'location' : job['location']}
            binds.append(bind)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
