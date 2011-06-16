#!/usr/bin/env python
"""
WorkQueue End Policy

"""



from WMCore.WMFactory import WMFactory

endFac = WMFactory(__name__, __name__)

def endPolicy(elements, parents = None, args = None):
    """Load an end policy"""

    # load policy and apply
    if not args:
        args = {}
    name = elements[0]['EndPolicy']['policyName']
    args = args.get(name, {})
    args.update(elements[0]['EndPolicy'])
    return endFac.loadObject(name,
                             args,
                             storeInCache = False)(elements, parents)

__all__ = [endPolicy]
