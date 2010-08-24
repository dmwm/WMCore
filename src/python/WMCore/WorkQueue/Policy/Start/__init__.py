#!/usr/bin/env python
"""
WorkQueue Start Policy

"""



from WMCore.WMFactory import WMFactory

startFac = WMFactory(__name__, __name__)

def startPolicy(name, startMap):
    """Load a start policy"""
    # Take splitting policy from workload & load specific policy for this queue
    # Workload may also directly specify specific splitter implementation
    if startMap.has_key(name):
        # take mapping from queue
        policyName = startMap[name]['name']
    else:
        # workload directly specifies splitting algo
        policyName = name
    try:
        args = startMap[name]['args']
    except IndexError:
        args = {}
    return startFac.loadObject(policyName,
                               args,
                               storeInCache = False,
                               )

__all__ = [startPolicy]
