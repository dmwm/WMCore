from __future__ import print_function, division

try:
    # This module has dependency with python binding for condor package (condor)
    import htcondor
except:
    pass


def isScheddOverloaded():
    """
    check whether job limit is reached in local schedd.
    Condition is check by following logic.
    ( ShadowsRunning > 9.700000000000000E-01 * MAX_RUNNING_JOBS) )
    || ( RecentDaemonCoreDutyCycle > 9.800000000000000E-01 )
    """
    coll = htcondor.Collector()
    scheddAd = coll.locate(htcondor.DaemonTypes.Schedd)
    return scheddAd['CurbMatchmaking'].eval()
