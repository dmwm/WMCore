#!/usr/bin/env python
"""
WorkQueue End Policy

"""

from WMCore.WMFactory import WMFactory

endFac = WMFactory(__name__, __name__)


def endPolicy(elements, parents=None, args=None):
    """Load an end policy"""

    # load policy and apply
    if not args:
        args = {}
    if elements:
        policy = elements[0]['EndPolicy']
    elif parents:
        policy = parents[0]['EndPolicy']
    else:
        raise RuntimeError("Can't get policy, no elements or parents")
    name = policy['policyName']
    args = args.get(name, {})
    args.update(policy)
    return endFac.loadObject(name,
                             args,
                             storeInCache=False)(elements, parents)


__all__ = []
