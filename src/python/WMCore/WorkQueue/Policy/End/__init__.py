#!/usr/bin/env python
"""
WorkQueue End Policy

"""
__revision__ = "$Id: __init__.py,v 1.2 2009/12/09 17:12:44 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMFactory import WMFactory
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

endFac = WMFactory(__name__, __name__)

def endPolicy(elements, args):
    """Load an end policy"""

    for ele in elements:
        # if still working return simple aggregate state
        if not ele.inEndState():
            return endFac.loadObject('EndPolicyInterface', {},
                                     storeInCache = False)(*elements)

    # all elements finished processing load policy and apply
    spec = WMWorkloadHelper()
    spec.load(elements[0]['WMSpecUrl'])
    [x.__setitem__('WMSpec', spec) for x in elements]
    name = spec.data.policies.end.policyName
    args = args.get(name, {})
    args.update(spec.data.policies.end.dictionary_())
    return endFac.loadObject(name,
                             args,
                             storeInCache = False)(*elements)

__all__ = [endPolicy]
