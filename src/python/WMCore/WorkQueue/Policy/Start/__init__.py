#!/usr/bin/env python
"""
WorkQueue Start Policy

"""
__revision__ = "$Id: __init__.py,v 1.3 2010/07/19 12:22:57 swakef Exp $"
__version__ = "$Revision: 1.3 $"

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
