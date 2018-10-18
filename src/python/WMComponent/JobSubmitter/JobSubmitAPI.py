from __future__ import print_function, division
import logging
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.PyCondor.PyCondorAPI import getScheddParamValue


def availableScheddSlots(dbi, logger=logging, condorFraction=1):
    """
    check executing jobs and compare with condor limit.
    return the difference -- executing jobs - (condor limit * condorFraction)
    """
    action = DAOFactory(package='WMCore.WMBS',
                        logger=logger,
                        dbinterface=dbi)(classname="Jobs.GetCountByState")
    executingJobs = int(action.execute("executing"))

    maxScheddJobs = getScheddParamValue("MAX_JOBS_PER_OWNER")

    if maxScheddJobs is None:
        logger.warning("Failed to retrieve 'MAX_JOBS_PER_OWNER' from HTCondor")
        return 0

    freeSubmitSlots = int(int(maxScheddJobs) * condorFraction - executingJobs)
    return freeSubmitSlots