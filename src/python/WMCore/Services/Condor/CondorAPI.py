"""
_CondorAPI_

Class used to interact with Condor daemons on the agent
"""

from __future__ import division
import htcondor
import logging


class CondorAPI(object):

    def __init__(self):
        self.schedd = htcondor.Schedd()

    def getCondorJobs(self, constraint, attr_list):
        """
        Return an error status and a list of job attributes from condor given a constraint.

        The error variable is used to differentiate between returning an empty list due to no jobs
        matching the constraint and returning an empty list due to an error querying the schedd
        """
        error = False
        jobs = []

        try:
            jobs = self.schedd.query(constraint, attr_list)
        except Exception as ex:
            error = True
            logging.error("CondorAPI schedd query failed.")
            logging.exception(ex)

        return error, jobs
