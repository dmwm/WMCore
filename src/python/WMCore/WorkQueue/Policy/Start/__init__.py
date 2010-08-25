#!/usr/bin/env python
"""
WorkQueue Start Policy

"""
__revision__ = "$Id: __init__.py,v 1.1 2009/12/02 13:52:43 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMFactory import WMFactory

startFac = WMFactory(__name__, __name__)

def startPolicy(name, startMap):
    """Load a start policy"""
    # Do we have a defined policy for this policy - if not load null policy
    #if not startMap.has_key(name):
    #    name = 'passthrough'
    policyName, args = startMap[name]
    return startFac.loadObject(policyName,
                               args,
                               storeInCache = False,
                               )

__all__ = [startPolicy]
