#!/usr/bin/env python
"""
WorkQueue End Policy

"""



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

    # all the spec should be same in the elements
    # all elements finished processing load policy and apply
    spec = elements[0]['WMSpec']
    name = spec.data.policies.end.policyName
    args = args.get(name, {})
    args.update(spec.data.policies.end.dictionary_())
    return endFac.loadObject(name,
                             args,
                             storeInCache = False)(*elements)

__all__ = [endPolicy]
