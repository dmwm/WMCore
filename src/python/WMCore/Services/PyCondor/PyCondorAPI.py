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
    """
    Some APIs to interact with HTCondor via the HTCondor python bindings.
    """
    def __init__(self):
        self.schedd = htcondor.Schedd()
        self.coll = htcondor.Collector()

    def recreateSchedd(self):
        """
        In case our current schedd object is in a "strange" state, we
        better recreate it.
        """
        logging.warning("Recreating Schedd instance due to query error...")
        self.schedd = htcondor.Schedd()

    def getCondorJobsSummary(self):
        """
        Retrieves a job summary from the HTCondor Schedd object
        :return: a list of classads representing the matching jobs, or None if failed
        """
        jobs = None  # return None to signalize the query failed
        queryOpts = htcondor.htcondor.QueryOpts.SummaryOnly
        try:
            return self.schedd.query(opts=queryOpts)
        except Exception:
            self.recreateSchedd()

        try:
            jobs = self.schedd.query(opts=queryOpts)
        except Exception as ex:
            logging.exception("Failed to fetch summary of jobs from Condor Schedd. Error: %s", str(ex))
        return jobs

    def getCondorJobs(self, constraint='true', attrList=None, limit=-1, opts="Default"):
        """
        Given a job/schedd constraint, return a list of job classad.
        :param constraint: the query constraint (str or ExprTree). Defaults to 'true'
        :param attrList: a list of attribute strings to be returned in the call.
                         It defaults to all attributes.
        :param limit: a limit on the number of matches to return. Defaults to -1 (all)
        :param opts: string with additional flags for the query. Defaults to Default.
            https://htcondor.readthedocs.io/en/v8_9_7/apis/python-bindings/api/htcondor.html#htcondor.QueryOpts
        :return: returns an iterator to the job classads
        """
        attrList = attrList or []
        # if option parameter is invalid, default it to the standard behavior
        opts = getattr(htcondor.htcondor.QueryOpts, opts, "Default")
        msg = "Querying condor schedd with params: constraint=%s, attrList=%s, limit=%s, opts=%s"
        logging.info(msg, constraint, attrList, limit, opts)
        try:
            return self.schedd.query(constraint, attrList, limit, opts=opts)
        except Exception:
            self.recreateSchedd()

        # if we hit another exception, let it be raised up in the chain
        return self.schedd.query(constraint, attrList, limit, opts=opts)

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
    paramResult = None
    if not isinstance(param, basestring):
        logging.error("Parameter %s must be string type", param)
        return paramResult

    try:
        paramResult = htcondor.param[param]
    except Exception as ex:
        msg = "Condor failed to fetch schedd parameter: %s" % param
        msg += "Error message: %s" % str(ex)
        logging.exception(msg)

    return paramResult
