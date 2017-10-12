"""
_PyCondorAPI_

Class used to interact with Condor daemons on the agent
"""

from __future__ import print_function, division

import logging

try:
    # This module has dependency with python binding for condor package (condor)
    import htcondor
except ImportError:
    pass


class PyCondorAPI(object):
    def __init__(self):
        self.schedd = htcondor.Schedd()
        self.coll = htcondor.Collector()

    def getCondorJobs(self, constraint, attr_list):
        """
        _getCondorJobs_

        Given a job/schedd constraint, return a list of jobs attributes
        or None if the query to condor fails.
        """
        try:
            jobs = self.schedd.query(constraint, attr_list)
        except Exception as ex:
            jobs = None  # return None to signalize the query failed
            msg = "Condor failed to fetch schedd constraints for: %s" % constraint
            msg += "Error message: %s" % str(ex)
            logging.exception(msg)

        return jobs

    def isScheddOverloaded(self):
        """
        check whether job limit is reached in local schedd.
        Condition is check by following logic.
        ( ShadowsRunning > 9.700000000000000E-01 * MAX_RUNNING_JOBS) )
        || ( RecentDaemonCoreDutyCycle > 9.800000000000000E-01 )
        """
        try:
            scheddAd = self.coll.locate(htcondor.DaemonTypes.Schedd)
            isOverloaded = scheddAd['CurbMatchmaking'].eval()
        except Exception as ex:
            msg = "Condor failed to fetch schedd attributes."
            msg += "Error message: %s" % str(ex)
            logging.exception(msg)
            # since it failed, assume it's overloaded
            isOverloaded = True

        return isOverloaded


def getScheddParamValue(param):
    """
    _getScheddParamValue_

    Given a schedd parameter, retrieve it's value with htcondor, e.g.:
    MAX_JOBS_RUNNING, MAX_JOBS_PER_OWNER, etc
    """
    if not isinstance(param, basestring):
        logging.error("Parameter %s must be string type", param)
        return

    try:
        paramResult = htcondor.param[param]
    except Exception as ex:
        msg = "Condor failed to fetch schedd parameter: %s" % param
        msg += "Error message: %s" % str(ex)
        logging.exception(msg)
        # since it has failed, just return None (not sure it's good?!?)
        paramResult = None

    return paramResult
