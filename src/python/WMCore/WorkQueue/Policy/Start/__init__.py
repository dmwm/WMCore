#!/usr/bin/env python
"""
WorkQueue Start Policy

"""
__revision__ = "$Id: __init__.py,v 1.2 2010/06/11 16:34:07 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMFactory import WMFactory

startFac = WMFactory(__name__, __name__)

def startPolicy(name, startMap):
    """Load a start policy"""
    # Do we have a defined policy for this policy - if not load null policy
    #if not startMap.has_key(name):
    #    name = 'passthrough'
    policyName = startMap[name]['name']
    args = startMap[name]['args']
    return startFac.loadObject(policyName,
                               args,
                               storeInCache = False,
                               )

__all__ = [startPolicy]
