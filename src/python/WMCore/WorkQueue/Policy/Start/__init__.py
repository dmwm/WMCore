#!/usr/bin/env python
"""
WorkQueue Start Policy

"""
from copy import deepcopy
from WMCore.WMFactory import WMFactory

startFac = WMFactory(__name__, __name__)


def startPolicy(name, startMap, rucioObj, logger=None):
    """Load a start policy"""
    # Take splitting policy from workload & load specific policy for this queue
    # Workload may also directly specify specific splitter implementation
    if name in startMap:
        # take mapping from queue
        policyName = startMap[name]['name']
    else:
        # workload directly specifies splitting algo
        policyName = name
    try:
        args = deepcopy(startMap[name]['args'])
    except IndexError:
        args = {}
    args['rucioObject'] = rucioObj
    args['logger'] = logger
    return startFac.loadObject(policyName,
                               args,
                               storeInCache=False,
                               )


__all__ = []
