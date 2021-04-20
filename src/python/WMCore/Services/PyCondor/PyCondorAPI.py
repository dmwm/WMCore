"""
_PyCondorAPI_

Class used to interact with Condor daemons on the agent
"""

from __future__ import print_function, division

from builtins import str, object
from past.builtins import basestring
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

    def getCondorJobs(self, constraint='true', attrList=None, opts="SummaryOnly"):
        """
        Given a job/schedd constraint, return a list of jobs attributes
        or None if the query to condor fails.
        :param constraint: the query constraint (str or ExprTree). Defaults to 'true'
        :param attrList: a list of attribute strings to be returned in the call.
                         It defaults to all attributes.
        :param opts: string with additional flags for the query. Defaults to only summary.
            https://htcondor.readthedocs.io/en/v8_9_7/apis/python-bindings/api/htcondor.html#htcondor.QueryOpts
        :return: a list of classads representing the matching jobs
        """
        attrList = attrList or []
        # if option parameter is invalid, default it to the standard behavior
        opts = getattr(htcondor.htcondor.QueryOpts, opts, "Default")
        msg = "Querying condor schedd with params: constraint=%s, attrList=%s, opts=%s"
        logging.info(msg, constraint, attrList, opts)
        try:
            jobs = self.schedd.query(constraint, attrList, opts=opts)
            return jobs
        except Exception:
            # if there is an error, try to recreate the schedd instance
            logging.info("Recreating Schedd instance due to query error...")
            self.schedd = htcondor.Schedd()
        try:
            jobs = self.schedd.query(constraint, attrList, opts=opts)
        except Exception as ex:
            jobs = None  # return None to signalize the query failed
            msg = "Condor failed to fetch schedd constraints for: %s" % constraint
            msg += "Error message: %s" % str(ex)
            logging.exception(msg)

        return jobs

    def editCondorJobs(self, job_spec, attr, value):
        """
        _editCondorJobs_

        Edit a set of condor jobs given an attribute and value
        job_spec can be a list of job IDs or a string specifying a constraint
        """
        success = False
        try:
            self.schedd.edit(job_spec, attr, value)
            success = True
        except Exception as ex:
            # edit doesn't distinguish between an error and not matching any jobs
            # check for this message and assume it just didn't match any jobs
            if isinstance(ex, RuntimeError) and str(ex) == "Unable to edit jobs matching constraint":
                success = True
                msg = "Condor constraint did not match any jobs. "
                msg += "Message from schedd: %s" % str(ex)
                logging.info(msg)
            else:
                msg = "Condor failed to edit the jobs. "
                msg += "Error message: %s" % str(ex)
                logging.exception(msg)

        return success

    def isScheddOverloaded(self):
        """
        check whether job limit is reached in local schedd.
        Condition is check by following logic.
        ( ShadowsRunning > 9.700000000000000E-01 * MAX_RUNNING_JOBS) )
        || ( RecentDaemonCoreDutyCycle > 9.800000000000000E-01 )
        """
        try:
            scheddAd = self.coll.locate(htcondor.DaemonTypes.Schedd)
            q = self.coll.query(htcondor.AdTypes.Schedd, 'Name == "%s"' % scheddAd['Name'],
                                projection=['CurbMatchmaking'])[0]
            isOverloaded = q['CurbMatchmaking'].eval()
            return isOverloaded
        except Exception:
            # if there is an error, try to recreate the collector instance
            logging.info("Recreating Collector instance due to query error...")
            self.coll = htcondor.Collector()
        try:
            scheddAd = self.coll.locate(htcondor.DaemonTypes.Schedd)
            q = self.coll.query(htcondor.AdTypes.Schedd, 'Name == "%s"' % scheddAd['Name'],
                                projection=['CurbMatchmaking'])[0]
            isOverloaded = q['CurbMatchmaking'].eval()
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
